package stations

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strconv"
	"time"

	"github.com/superscale/spire/devices"
	"github.com/superscale/spire/mqtt"
)

// Key ...
const Key = "stations"

// LanStation ...
type LanStation struct {
	Vendor        string        `json:"vendor"`
	MAC           string        `json:"mac"`
	IP            string        `json:"ip"`
	Port          string        `json:"port"`
	Mode          string        `json:"mode"`
	Local         bool          `json:"local"`
	Age           float64       `json:"age"`
	Seen          int64         `json:"seen"`
	InactiveTime  time.Duration `json:"inactive_time"`
	LastUpdatedAt time.Time     `json:"-"`
}

// Thing ...
type Thing struct {
	Vendor        string                 `json:"vendor"`
	MAC           string                 `json:"mac"`
	IP            string                 `json:"ip"`
	Port          string                 `json:"port"`
	Mode          string                 `json:"mode"`
	Local         bool                   `json:"local"`
	Age           float64                `json:"age"`
	Seen          int64                  `json:"seen"`
	InactiveTime  time.Duration          `json:"inactive_time"`
	LastUpdatedAt time.Time              `json:"-"`
	Thing         map[string]interface{} `json:"thing"`
}

// State contains all data the stations handler needs to compile messages
// for the support frontend.
type State struct {
	WifiStations map[string]WifiStation // MAC -> WifiStation
	LanStations  map[string]*LanStation // MAC -> LanStation
	Things       map[string]*Thing      // IP -> Thing
}

// NewState ...
func NewState() *State {
	return &State{
		WifiStations: make(map[string]WifiStation),
		LanStations:  make(map[string]*LanStation),
		Things:       make(map[string]*Thing),
	}
}

// Message is the data structure published to the MQTT bus for control/UI clients to consume.
type Message struct {
	Public  []WifiStation `json:"public"`
	Private []WifiStation `json:"private"`
	Other   []*LanStation `json:"other"`
	Thing   []*Thing      `json:"thing"`
}

// Handler ...
type Handler struct {
	broker     *mqtt.Broker
	formations *devices.FormationMap
}

// Register ...
func Register(broker *mqtt.Broker, formations *devices.FormationMap) interface{} {
	h := &Handler{broker: broker, formations: formations}

	broker.Subscribe("pylon/+/wifi/poll", h)
	broker.Subscribe("pylon/+/wifi/event", h)
	broker.Subscribe("pylon/+/things/discovery", h)
	broker.Subscribe("pylon/+/net", h)
	broker.Subscribe("pylon/+/sys/facts", h)
	broker.Subscribe("pylon/+/odhcpd", h)
	broker.Subscribe(mqtt.SubscribeEventTopic, h)

	return h
}

// HandleMessage ...
func (h *Handler) HandleMessage(topic string, message interface{}) error {

	if topic == mqtt.SubscribeEventTopic {
		h.formations.RLock()
		defer h.formations.RUnlock()
		return h.onSubscribeEvent(message.(mqtt.SubscribeMessage))
	}

	t := devices.ParseTopic(topic)

	h.formations.Lock()
	defer h.formations.Unlock()

	switch t.Path {
	case "wifi/poll":
		msg, err := unmarshalWifiPollMessage(message)
		if err != nil {
			return err
		}
		return h.onWifiPollMessage(t, msg)
	case "wifi/event":
		msg, err := unmarshalWifiEventMessage(message)
		if err != nil {
			return err
		}
		return h.onWifiEventMessage(t, msg)
	case "things/discovery":
		msg, err := unmarshalThingsMessage(message)
		if err != nil {
			return err
		}
		return h.onThingsMessage(t, msg)
	case "net":
		msg, err := unmarshalNetMessage(message)
		if err != nil {
			return err
		}
		return h.onNetMessage(t, msg)
	case "sys/facts":
		msg, err := unmarshalSysMessage(message)
		if err != nil {
			return err
		}
		return h.onSysMessage(t, msg)
	case "odhcpd":
		buf, ok := message.([]byte)
		if !ok {
			return fmt.Errorf("[stations] expected byte buffer, got this instead: %v", message)
		}
		return h.onDHCPMessage(t, buf)
	default:
		return nil
	}
}

func subscribeFilter(path string) bool {
	return path == "#" || path == "stations"
}

func (h *Handler) onSubscribeEvent(sm mqtt.SubscribeMessage) error {

	for _, t := range devices.FilterSubscribeTopics(sm, subscribeFilter) {
		if state, ok := h.formations.GetDeviceState(t.DeviceName, Key).(*State); ok {
			h.publish(t.DeviceName, state)
		}
	}
	return nil
}

func (h *Handler) onWifiPollMessage(t devices.Topic, msg *WifiPollMessage) error {
	state, formationID := h.getState(t.DeviceName)
	h.updateWifiStations(msg, state, t.DeviceName)
	h.formations.PutState(formationID, Key, state)

	if surveyMsg, err := compileWifiSurveyMessage(msg); err != nil {
		return err
	} else if len(surveyMsg) > 0 {
		surveyTopic := fmt.Sprintf("matriarch/%s/wifi/survey", t.DeviceName)
		h.broker.Publish(surveyTopic, surveyMsg)
	}

	h.publish(t.DeviceName, state)
	return nil
}

func (h *Handler) onWifiEventMessage(t devices.Topic, msg *WifiEventMessage) error {
	state, formationID := h.getState(t.DeviceName)

	if msg.Action == "assoc" {
		state.WifiStations[msg.MAC] = WifiStation{"mac": msg.MAC}
	} else if msg.Action == "disassoc" {
		delete(state.WifiStations, msg.MAC)
	}

	h.formations.PutState(formationID, Key, state)
	h.publish(t.DeviceName, state)
	return nil
}

func (h *Handler) updateWifiStations(msg *WifiPollMessage, state *State, deviceName string) {

	for ifaceName, iface := range msg.Interfaces {

		stations, err := ParseWifiStations(iface.Stations, ifaceName)

		if err == nil {
			state.WifiStations = merge(state.WifiStations, stations)
		} else {
			log.Printf("[stations] error while parsing wifi station info for interface %s on device %s: %v", ifaceName, deviceName, err)
		}
	}
}

func (h *Handler) getState(deviceName string) (*State, string) {
	formationID := h.formations.FormationID(deviceName)
	state, ok := h.formations.GetState(formationID, Key).(*State)
	if !ok {
		return NewState(), formationID
	}

	return state, formationID
}

func (h *Handler) onThingsMessage(t devices.Topic, msg map[string]interface{}) error {
	ip, ipOk := msg["address"].(string)
	thingData, tOk := msg["thing"].(map[string]interface{})
	if !ipOk || !tOk {
		return fmt.Errorf("[stations] got invalid things discovery message: %v", msg)
	}

	state, formationID := h.getState(t.DeviceName)

	thing, exists := state.Things[ip]
	if exists {
		thing.Thing = thingData
		thing.LastUpdatedAt = time.Now().UTC()
	} else {
		thing := &Thing{
			IP:            ip,
			LastUpdatedAt: time.Now().UTC(),
			Thing:         thingData,
			Mode:          "thing",
		}

		state.Things[ip] = thing
	}

	h.formations.PutState(formationID, Key, state)
	h.publish(t.DeviceName, state)
	return nil
}

type netMessage struct {
	MAC []struct {
		MAC string `json:"mac"`
		IP  string `json:"ip"`
	} `json:"mac"`

	Bridge struct {
		MACs struct {
			Public  string `json:"public"`
			Private string `json:"private"`
		} `json:"macs"`
	} `json:"bridge"`

	Switch string `json:"switch"`
}

func (h *Handler) onNetMessage(t devices.Topic, msg *netMessage) error {
	state, formationID := h.getState(t.DeviceName)
	now := time.Now().UTC()

	for _, e := range msg.MAC {
		if thing := state.Things[e.IP]; thing != nil {
			thing.MAC = e.MAC
			thing.Vendor = vendorFromMAC(e.MAC)
			thing.LastUpdatedAt = now
		} else {
			if station, exists := state.WifiStations[e.MAC]; exists {
				station["ip"] = e.IP
			} else {
				state.LanStations[e.MAC] = &LanStation{
					Vendor:        vendorFromMAC(e.MAC),
					MAC:           e.MAC,
					IP:            e.IP,
					Mode:          "other",
					LastUpdatedAt: now,
				}
			}
		}
	}

	if err := h.assignPorts(msg, t.DeviceName, state); err != nil {
		log.Printf("[stations] error while assigning ports from switch info for device %s: %v", t.DeviceName, err)
	}

	if err := h.assignBridgeInfo(msg, t.DeviceName, state); err != nil {
		log.Printf("[stations] error while assigning bridge info for device %s: %v", t.DeviceName, err)
	}

	h.removeTimedOutStations(state)
	h.formations.PutState(formationID, Key, state)
	h.publish(t.DeviceName, state)
	return nil
}

const lanStationTimeout = time.Minute * 10
const thingTimeout = time.Minute * 5

func (h *Handler) removeTimedOutStations(state *State) {
	now := time.Now().UTC()

	for mac, ls := range state.LanStations {
		ls.InactiveTime = now.Sub(ls.LastUpdatedAt)
		if ls.InactiveTime > lanStationTimeout {
			delete(state.LanStations, mac)
		}
	}

	for ip, thing := range state.Things {
		thing.InactiveTime = now.Sub(thing.LastUpdatedAt)
		if thing.InactiveTime > thingTimeout {
			delete(state.Things, ip)
		}
	}
}

func (h *Handler) assignPorts(msg *netMessage, deviceName string, state *State) error {
	var mac2port map[string]string
	var err error
	cpuPorts, ok := h.formations.GetDeviceState(deviceName, "cpu_ports").([]string)
	if ok {
		_, mac2port, err = ParseSwitch(msg.Switch, cpuPorts...)
	} else {
		_, mac2port, err = ParseSwitch(msg.Switch)
	}
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	for mac, ws := range state.WifiStations {
		if port, exists := mac2port[mac]; exists {
			ws["port"] = port
		}
	}

	for mac, ls := range state.LanStations {
		if port, exists := mac2port[mac]; exists {
			ls.Port = port
			ls.LastUpdatedAt = now
		}
	}

	for _, t := range state.Things {
		if len(t.MAC) == 0 {
			continue
		}

		if port, exists := mac2port[t.MAC]; exists {
			t.Port = port
			t.LastUpdatedAt = now
		}
	}

	return nil
}

func (h *Handler) assignBridgeInfo(msg *netMessage, deviceName string, state *State) error {
	bridgeInfo, err := ParseBridgeMACs(msg.Bridge.MACs.Private)
	if err != nil {
		return err
	}
	pubBI, err := ParseBridgeMACs(msg.Bridge.MACs.Public)
	if err != nil {
		return err
	}
	for m, bi := range pubBI {
		bridgeInfo[m] = bi
	}

	now := time.Now().UTC()
	for mac, ws := range state.WifiStations {
		if bi, exists := bridgeInfo[mac]; exists {
			ws["age"] = bi.Age
			ws["local"] = bi.Local
		}
	}

	for mac, ls := range state.LanStations {
		if bi, exists := bridgeInfo[mac]; exists {
			ls.Age = bi.Age
			ls.Local = bi.Local
			ls.LastUpdatedAt = now
		}
	}

	for _, t := range state.Things {
		if len(t.MAC) == 0 {
			continue
		}

		if bi, exists := bridgeInfo[t.MAC]; exists {
			t.Age = bi.Age
			t.Local = bi.Local
			t.LastUpdatedAt = now
		}
	}

	return nil
}

type sysMessage struct {
	Board struct {
		Switch struct {
			Switch0 struct {
				Ports []struct {
					Number int     `json:"num"`
					Device *string `json:"device"`
				} `json:"ports"`
			} `json:"switch0"`
		} `json:"switch"`
	} `json:"board"`
}

func (h *Handler) onSysMessage(t devices.Topic, msg *sysMessage) error {
	cpuPorts := []string{}
	for _, port := range msg.Board.Switch.Switch0.Ports {
		if port.Device != nil {
			cpuPorts = append(cpuPorts, strconv.Itoa(port.Number))
		}
	}

	h.formations.PutDeviceState(h.formations.FormationID(t.DeviceName), t.DeviceName, "cpu_ports", cpuPorts)
	return nil
}

func (h *Handler) onDHCPMessage(t devices.Topic, msg []byte) error {
	dhcpState, err := ParseDHCP(msg)
	if err != nil {
		return err
	}

	h.broker.Publish(fmt.Sprintf("matriarch/%s/dhcp/leases", t.DeviceName), dhcpState)
	return nil
}

func round(f float64) int64 {
	return int64(math.Floor(f + 0.5))
}

func (h *Handler) publish(deviceName string, state *State) {
	now := time.Now().UTC().Unix()

	msg := &Message{
		Public:  []WifiStation{},
		Private: []WifiStation{},
		Other:   make([]*LanStation, len(state.LanStations)),
		Thing:   []*Thing{},
	}

	for _, station := range state.WifiStations {
		if age, ok := station["age"].(float64); ok {
			station["seen"] = now - round(age)
		}

		if station["mode"] == "public" {
			msg.Public = append(msg.Public, station)
		} else {
			msg.Private = append(msg.Private, station)
		}
	}

	for _, thing := range state.Things {
		if len(thing.MAC) > 0 {
			thing.Seen = now - round(thing.Age)
			msg.Thing = append(msg.Thing, thing)
		}
	}

	i := 0
	for _, station := range state.LanStations {
		station.Seen = now - round(station.Age)
		msg.Other[i] = station
		i++
	}

	h.broker.Publish(fmt.Sprintf("matriarch/%s/stations", deviceName), msg)
}

func unmarshalWifiPollMessage(payload interface{}) (*WifiPollMessage, error) {
	buf, ok := payload.([]byte)
	if !ok {
		return nil, fmt.Errorf("[stations] expected byte buffer, got this instead: %v", payload)
	}

	msg := new(WifiPollMessage)
	if err := json.Unmarshal(buf, msg); err != nil {
		return nil, err
	}

	return msg, nil
}

func unmarshalWifiEventMessage(payload interface{}) (*WifiEventMessage, error) {
	buf, ok := payload.([]byte)
	if !ok {
		return nil, fmt.Errorf("[stations] expected byte buffer, got this instead: %v", payload)
	}

	msg := new(WifiEventMessage)
	if err := json.Unmarshal(buf, msg); err != nil {
		return nil, err
	}

	return msg, nil
}

func unmarshalThingsMessage(payload interface{}) (map[string]interface{}, error) {
	buf, ok := payload.([]byte)
	if !ok {
		return nil, fmt.Errorf("[stations] expected byte buffer, got this instead: %v", payload)
	}

	var msg map[string]interface{}
	if err := json.Unmarshal(buf, &msg); err != nil {
		return nil, err
	}

	return msg, nil
}

func unmarshalNetMessage(payload interface{}) (*netMessage, error) {
	buf, ok := payload.([]byte)
	if !ok {
		return nil, fmt.Errorf("[stations] expected byte buffer, got this instead: %v", payload)
	}

	msg := new(netMessage)
	if err := json.Unmarshal(buf, msg); err != nil {
		return nil, err
	}

	return msg, nil
}

func unmarshalSysMessage(payload interface{}) (*sysMessage, error) {
	buf, ok := payload.([]byte)
	if !ok {
		return nil, fmt.Errorf("[stations] expected byte buffer, got this instead: %v", payload)
	}

	msg := new(sysMessage)
	if err := json.Unmarshal(buf, msg); err != nil {
		return nil, err
	}

	return msg, nil
}
