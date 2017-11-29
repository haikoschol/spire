package up

import (
	"context"
	"fmt"
	"time"

	"github.com/superscale/spire/devices"
	"github.com/superscale/spire/mqtt"
)

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
	ctx, cancelFn := context.WithCancel(context.Background())
	h.formations.PutDeviceState(cm.FormationID, cm.DeviceName, "cancelUpFn", cancelFn)

	go h.publishUpState(ctx, cm.DeviceName)
	return nil
}

func (h *Handler) onDisconnect(dm devices.DisconnectMessage) error {
	r := h.formations.GetDeviceState(dm.DeviceName, "cancelUpFn")
	cancelFn, ok := r.(context.CancelFunc)
	if !ok {
		return fmt.Errorf("cannot cancel goroutine that publishes 'up' state for device %s", dm.DeviceName)
	}

	cancelFn()

	h.formations.DeleteDeviceState(dm.FormationID, dm.DeviceName, "cancelUpFn")
	return nil
}

func subscribeFilter(path string) bool {
	return path == "up" || path == "#"
}

func (h *Handler) onSubscribeEvent(sm mqtt.SubscribeMessage) error {

	for _, t := range devices.FilterSubscribeTopics(sm, subscribeFilter) {
		if h.formations.GetDeviceState(t.DeviceName, "cancelUpFn") != nil {
			h.publishUpMsg(t.DeviceName, upState)
		} else {
			h.publishUpMsg(t.DeviceName, downState)
		}
	}

	return nil
}

func (h *Handler) publishUpState(ctx context.Context, deviceName string) {
	h.publishUpMsg(deviceName, upState)

	for {
		select {
		case <-ctx.Done():
			h.publishUpMsg(deviceName, downState)
			return
		case <-time.After(30 * time.Second):
			h.publishUpMsg(deviceName, upState)
		}
	}
}

const upState = "up"
const downState = "down"

func (h *Handler) publishUpMsg(deviceName, state string) {
	topic := fmt.Sprintf("matriarch/%s/up", deviceName)

	msg := map[string]interface{}{
		"state":     state,
		"timestamp": time.Now().UTC().Unix(),
	}

	h.broker.Publish(topic, msg)
}
