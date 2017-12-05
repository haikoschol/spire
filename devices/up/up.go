package up

import (
	"fmt"
	"time"

	"github.com/superscale/spire/devices"
	"github.com/superscale/spire/mqtt"
)

type state string

const (
	// Key for the device state managed by this handler
	Key = "up"

	// Up ...
	Up state = "up"

	// Down ...
	Down state = "down"
)

// Message represents both the device state stored in memory and the format of
// the messages published to the UI.
type Message struct {
	State     state `json:"state"`
	Timestamp int64 `json:"timestamp"`
}

// Handler ...
type Handler struct {
	broker     *mqtt.Broker
	formations *devices.FormationMap
}

// Register ...
func Register(broker *mqtt.Broker, formations *devices.FormationMap) interface{} {
	h := &Handler{broker, formations}

	broker.Subscribe(devices.ConnectTopic.String(), h)
	broker.Subscribe(devices.DisconnectTopic.String(), h)
	broker.Subscribe(mqtt.SubscribeEventTopic, h)
	return h
}

// HandleMessage ...
func (h *Handler) HandleMessage(topic string, message interface{}) error {
	h.formations.Lock()
	defer h.formations.Unlock()

	t := devices.ParseTopic(topic)

	if t.Path == devices.ConnectTopic.Path {
		return h.onConnect(message.(devices.ConnectMessage))
	} else if t.Path == devices.DisconnectTopic.Path {
		return h.onDisconnect(message.(devices.DisconnectMessage))
	} else if topic == mqtt.SubscribeEventTopic {
		return h.onSubscribeEvent(message.(mqtt.SubscribeMessage))
	}

	return nil
}

func (h *Handler) onConnect(cm devices.ConnectMessage) error {
	msg := Message{
		State:     Up,
		Timestamp: time.Now().UTC().Unix(),
	}

	h.formations.PutDeviceState(cm.FormationID, cm.DeviceName, Key, msg)
	h.sendToUI(cm.DeviceName, msg)

	return nil
}

func (h *Handler) onDisconnect(dm devices.DisconnectMessage) error {
	msg := Message{
		State:     Down,
		Timestamp: time.Now().UTC().Unix(),
	}

	h.formations.PutDeviceState(dm.FormationID, dm.DeviceName, Key, msg)
	h.sendToUI(dm.DeviceName, msg)

	return nil
}

func subscribeFilter(path string) bool {
	return path == "up" || path == "#"
}

func (h *Handler) onSubscribeEvent(sm mqtt.SubscribeMessage) error {

	for _, t := range devices.FilterSubscribeTopics(sm, subscribeFilter) {
		if msg, ok := h.formations.GetDeviceState(t.DeviceName, Key).(Message); ok {
			h.sendToUI(t.DeviceName, msg)
		} else {
			h.sendToUI(t.DeviceName, Message{State: Down})
		}
	}

	return nil
}

func (h *Handler) sendToUI(deviceName string, msg Message) {
	topic := fmt.Sprintf("matriarch/%s/up", deviceName)
	h.broker.Publish(topic, msg)
}
