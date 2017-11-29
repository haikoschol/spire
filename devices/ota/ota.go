package ota

import (
	"encoding/json"
	"fmt"

	"github.com/superscale/spire/devices"
	"github.com/superscale/spire/mqtt"
)

type states int

const (
	// Default ...
	Default states = iota
	// Downloading ...
	Downloading
	// Upgrading ...
	Upgrading
	// Error ...
	Error
	// Cancelled ...
	Cancelled
)

// Message ...
type Message struct {
	State    states `json:"state"`    // <"downloading"|"upgrading"|"error">,
	Progress int    `json:"progress"` // <download progress in percent> if state is "downloading"
	Error    string `json:"error"`
	Yours    string `json:"yours"` // if error is "sha256 mismatch"
	Mine     string `json:"mine"`  // if error is "sha256 mismatch"
}

// Handler ...
type Handler struct {
	broker     *mqtt.Broker
	formations *devices.FormationMap
}

const formationCacheKey = "ota"
const stateTopicPath = "ota/state"
const upgradeTopicPath = "ota/sysupgrade"
const cancelTopicPath = "ota/cancel"

// Register ...
func Register(broker *mqtt.Broker, formations *devices.FormationMap) interface{} {
	h := &Handler{broker, formations}

	broker.Subscribe(devices.ConnectTopic.String(), h)
	broker.Subscribe(devices.DisconnectTopic.String(), h)
	broker.Subscribe("pylon/+/"+stateTopicPath, h)
	broker.Subscribe("armada/+/ota/#", h)
	broker.Subscribe(mqtt.SubscribeEventTopic, h)
	return h
}

// HandleMessage implements mqtt.Subscriber
func (h *Handler) HandleMessage(topic string, message interface{}) error {
	h.formations.Lock()
	defer h.formations.Unlock()

	t := devices.ParseTopic(topic)

	if t.Path == devices.ConnectTopic.Path {
		return h.onConnect(message.(devices.ConnectMessage))
	}

	if t.Path == devices.DisconnectTopic.Path {
		return h.onDisconnect(message.(devices.DisconnectMessage))
	}

	if t.Path == cancelTopicPath {
		h.forwardAndUpdateState(t, message, Cancelled)
		return nil
	}

	if topic == mqtt.SubscribeEventTopic {
		return h.onSubscribeEvent(message.(mqtt.SubscribeMessage))
	}

	buf, ok := message.([]byte)
	if !ok {
		return fmt.Errorf("[OTA] expected byte buffer, got this instead: %v", message)
	}

	if t.Path == stateTopicPath {
		return h.onStateMessage(t, buf)
	}

	if t.Path == upgradeTopicPath {
		return h.onUpgradeMessage(t, buf)
	}

	return nil
}

func (h *Handler) onStateMessage(topic devices.Topic, buf []byte) error {
	msg := new(Message)
	if err := json.Unmarshal(buf, msg); err != nil {
		return err
	}

	if msg.State != Downloading {
		formationID := h.formations.FormationID(topic.DeviceName)
		h.formations.PutDeviceState(formationID, topic.DeviceName, formationCacheKey, msg)
	}

	h.sendToUI(topic.DeviceName, msg)
	return nil
}

func (h *Handler) onUpgradeMessage(topic devices.Topic, buf []byte) error {
	msg := make(map[string]interface{})
	if err := json.Unmarshal(buf, &msg); err != nil {
		return err
	}

	if err := checkStrings(msg, "url", "sha256"); err != nil {
		return err
	}

	h.forwardAndUpdateState(topic, buf, Downloading)
	return nil
}

func (h *Handler) onConnect(cm devices.ConnectMessage) error {
	msg := &Message{State: Default}
	h.formations.PutDeviceState(cm.FormationID, cm.DeviceName, formationCacheKey, msg)
	h.sendToUI(cm.DeviceName, msg)
	return nil
}

func (h *Handler) onDisconnect(dm devices.DisconnectMessage) error {
	rawState := h.formations.GetDeviceState(dm.DeviceName, formationCacheKey)
	state, ok := rawState.(*Message)

	if ok && state.State == Downloading {
		h.sendToUI(dm.DeviceName, &Message{State: Error, Error: "connection to device lost during download"})
	}

	return nil
}

func subscribeFilter(path string) bool {
	return path == "#" || path == stateTopicPath
}

func (h *Handler) onSubscribeEvent(sm mqtt.SubscribeMessage) error {

	for _, t := range devices.FilterSubscribeTopics(sm, subscribeFilter) {
		state, _ := h.formations.GetDeviceState(t.DeviceName, formationCacheKey).(*Message)
		h.sendToUI(t.DeviceName, state)
	}
	return nil
}

func (h *Handler) sendToUI(deviceName string, msg *Message) {
	if msg == nil {
		msg = &Message{State: Default}
	}

	topic := fmt.Sprintf("matriarch/%s/%s", deviceName, stateTopicPath)
	h.broker.Publish(topic, msg)
}

func (h *Handler) sendToDevice(topic devices.Topic, msg interface{}) {
	topic.Prefix = "pylon"
	h.broker.Publish(topic.String(), msg)
}

func (h *Handler) forwardAndUpdateState(topic devices.Topic, message interface{}, state states) {
	h.sendToDevice(topic, message)

	formationID := h.formations.FormationID(topic.DeviceName)
	stateMsg := &Message{State: state}

	h.sendToUI(topic.DeviceName, stateMsg)
	h.formations.PutDeviceState(formationID, topic.DeviceName, formationCacheKey, stateMsg)
}

func (s states) String() string {
	switch s {
	case Default:
		return "default"
	case Downloading:
		return "downloading"
	case Upgrading:
		return "upgrading"
	case Error:
		return "error"
	case Cancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

// MarshalJSON ...
func (s *Message) MarshalJSON() ([]byte, error) {
	type Alias Message
	tmp := &struct {
		State string `json:"state"`
		*Alias
	}{
		State: s.State.String(),
		Alias: (*Alias)(s),
	}
	return json.Marshal(tmp)
}

// UnmarshalJSON ...
func (s *Message) UnmarshalJSON(data []byte) error {
	type Alias Message
	tmp := &struct {
		State string `json:"state"`
		*Alias
	}{
		Alias: (*Alias)(s),
	}
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	switch tmp.State {
	case "downloading":
		s.State = Downloading
	case "upgrading":
		s.State = Upgrading
	case "error":
		s.State = Error
	case "cancelled":
		s.State = Cancelled
	default:
		s.State = Default
	}

	return nil
}

func checkStrings(m map[string]interface{}, keys ...string) error {
	for _, key := range keys {
		raw, exists := m[key]
		_, isStr := raw.(string)
		if !exists || !isStr {
			return fmt.Errorf("[OTA] corrupt sysupgrade message: '%s' missing or not a string: %v", key, m)
		}
	}

	return nil
}
