package exception

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/bugsnag/bugsnag-go"
	bugsnagErrors "github.com/bugsnag/bugsnag-go/errors"
	"github.com/superscale/spire/config"
	"github.com/superscale/spire/devices"
	"github.com/superscale/spire/mqtt"
)

// Message ...
type Message struct {
	Error   string `json:"error"`
	Context string `json:"context"`
}

// Handler ...
type Handler struct {
	formations *devices.FormationMap
}

// Register ...
func Register(broker *mqtt.Broker, formations *devices.FormationMap) interface{} {
	h := &Handler{formations}
	broker.Subscribe("pylon/+/exception", h)
	return h
}

// HandleMessage ...
func (h *Handler) HandleMessage(topic string, payload interface{}) error {
	if len(config.Config.BugsnagKey) == 0 {
		return errors.New("[exception] bugsnag API key not set")
	}

	buf, ok := payload.([]byte)
	if !ok {
		return fmt.Errorf("[exception] expected byte buffer, got this instead: %v", payload)
	}

	m := Message{Error: "unknown exception on device", Context: "unknown originating topic"}
	if err := json.Unmarshal(buf, &m); err != nil {
		return bugsnagErrors.New(err, 1)
	}

	t := devices.ParseTopic(topic)

	ctx := bugsnag.Context{"pylon:" + m.Context}

	metadata := bugsnag.MetaData{"device": {
		"name":      t.DeviceName,
		"osVersion": h.getDeviceOS(t.DeviceName),
	}}

	notifier := bugsnag.New(bugsnag.Configuration{
		APIKey:       config.Config.BugsnagKey,
		ReleaseStage: config.Config.Environment,
	})

	return notifier.Notify(errors.New(m.Error), bugsnag.SeverityError, ctx, metadata)
}

func (h *Handler) getDeviceOS(deviceName string) (deviceOS string) {
	h.formations.RLock()
	defer h.formations.RUnlock()

	deviceOS = "unknown"
	rawState := h.formations.GetDeviceState(deviceName, "device_info")

	deviceInfo, ok := rawState.(map[string]interface{})
	if !ok {
		return
	}

	raw, exists := deviceInfo["device_os"]
	if !exists {
		return
	}

	if s, isStr := raw.(string); isStr {
		return s
	}

	return
}
