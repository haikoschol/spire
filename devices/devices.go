package devices

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"strings"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/superscale/spire/config"
	"github.com/superscale/spire/mqtt"
)

// ConnectTopic ...
var ConnectTopic = Topic{Prefix: mqtt.InternalTopicPrefix, Path: "spire/devices/connect"}

// DisconnectTopic ...
var DisconnectTopic = Topic{Prefix: mqtt.InternalTopicPrefix, Path: "spire/devices/disconnect"}

// ConnectMessage ...
type ConnectMessage struct {
	FormationID string `json:"formation_id"`
	DeviceName  string
	DeviceInfo  map[string]interface{}
	IPAddress   string `json:"ip_address"`
}

// DisconnectMessage ...
type DisconnectMessage struct {
	FormationID string
	DeviceName  string
}

// Handler ...
type Handler struct {
	formations *FormationMap
	broker     *mqtt.Broker
}

// NewHandler ...
func NewHandler(formations *FormationMap, broker *mqtt.Broker) *Handler {
	return &Handler{
		formations: formations,
		broker:     broker,
	}
}

// Topic ...
type Topic struct {
	Prefix     string
	DeviceName string
	Path       string
}

func (t Topic) String() string {
	parts := []string{t.Prefix}

	if len(t.DeviceName) > 0 {
		parts = append(parts, t.DeviceName)
	}

	if len(t.Path) > 0 {
		parts = append(parts, t.Path)
	}

	return strings.Join(parts, "/")
}

// ParseTopic ...
func ParseTopic(topic string) Topic {
	if strings.HasPrefix(topic, "/") {
		topic = topic[1:]
	}

	if strings.HasPrefix(topic, mqtt.InternalTopicPrefix) {
		parts := strings.SplitN(topic, "/", 2)
		return Topic{parts[0], "", parts[1]}
	}

	parts := strings.SplitN(topic, "/", 3)
	return Topic{parts[0], parts[1], parts[2]}
}

// FilterSubscribeTopics filters all topics in a SubscribeEventMessage
// through a predicate and returns the matches as a slice of Topic objects.
// The predicate receives the "path" component of the topic as argument.
// Topics with a prefix other than "matriarch" as well as those with
// wildcards in the device name part will be skipped.
func FilterSubscribeTopics(sm mqtt.SubscribeMessage, matches func(string) bool) []Topic {
	matchingTopics := []Topic{}

	for _, topic := range sm.Topics {
		t := ParseTopic(topic)

		if t.Prefix == "matriarch" && t.DeviceName != "+" && matches(t.Path) {
			matchingTopics = append(matchingTopics, t)
		}
	}

	return matchingTopics
}

// HandleConnection ...
func (h *Handler) HandleConnection(session *mqtt.Session) {

	cm, err := h.connect(session)
	if err != nil {
		if err != io.EOF {
			log.Println("could not establish a session. closing connection.", err)
			session.Close()
		}
		return
	}

	for {
		ca, err := session.Read()
		if err != nil {
			if err != io.EOF {
				log.Printf("error while reading packet from %s: %v. closing connection", cm.DeviceName, err)
			}

			h.deviceDisconnected(cm.FormationID, cm.DeviceName, session)
			return
		}

		switch ca := ca.(type) {
		case *packets.PingreqPacket:
			err = session.SendPingresp()
		case *packets.PublishPacket:
			h.broker.Publish(ca.TopicName, ca.Payload)
		case *packets.SubscribePacket:
			err = h.broker.HandleSubscribePacket(ca, session, false)
		case *packets.UnsubscribePacket:
			h.broker.UnsubscribeAll(ca, session)
			err = session.SendUnsuback(ca.MessageID)
		case *packets.DisconnectPacket:
			h.deviceDisconnected(cm.FormationID, cm.DeviceName, session)
			return
		default:
			log.Println("ignoring unsupported message from", cm.DeviceName)
		}

		if err != nil {
			log.Printf("error while handling packet from device %s (%v): %v", cm.DeviceName, session.RemoteAddr(), err)
		}
	}
}

func (h *Handler) connect(session *mqtt.Session) (*ConnectMessage, error) {
	pkg, err := session.ReadConnect()
	if err != nil {
		return nil, err
	}

	cm, err := buildConnectMessage(pkg, session)
	if err != nil {
		return nil, err
	}

	h.formations.Lock()
	h.formations.AddDevice(cm.DeviceName, cm.FormationID)
	h.formations.Unlock()

	if err = session.AcknowledgeConnect(); err != nil {
		return nil, err
	}

	h.broker.Publish(ConnectTopic.String(), *cm)
	return cm, nil
}

func (h *Handler) deviceDisconnected(formationID, deviceName string, session *mqtt.Session) {
	h.broker.Remove(session)

	if err := session.Close(); err != nil {
		log.Println(err)
	}

	h.broker.Publish(DisconnectTopic.String(), DisconnectMessage{formationID, deviceName})
}

func buildConnectMessage(pkg *packets.ConnectPacket, session *mqtt.Session) (cm *ConnectMessage, err error) {
	cm = &ConnectMessage{DeviceName: pkg.ClientIdentifier}
	if err = json.Unmarshal([]byte(pkg.Username), cm); err != nil {
		return
	}

	if len(cm.FormationID) == 0 {
		return nil, fmt.Errorf("CONNECT packet from %v is missing formation ID. closing connection", session.RemoteAddr())
	}

	if cm.DeviceInfo, err = fetchDeviceInfo(cm.DeviceName); err != nil {
		cm = nil
	}
	return
}

func fetchDeviceInfo(deviceName string) (map[string]interface{}, error) {
	url := fmt.Sprintf("%s/v2/devices/%s", config.Config.LiberatorBaseURL, deviceName)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Authorization", "Bearer "+config.Config.LiberatorJWTToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	info := make(map[string]interface{})
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		err := fmt.Errorf("unexpected response from liberator for device %s. status: %d %s. error: %v",
			deviceName, resp.StatusCode, resp.Status, info["error"])

		return nil, err
	}

	return info, nil
}

// Round ...
func Round(f, places float64) float64 {
	shift := math.Pow(10, places)
	f = math.Floor(f*shift + .5)
	return f / shift
}
