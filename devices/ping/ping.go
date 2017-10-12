package ping

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/superscale/spire/devices"
	"github.com/superscale/spire/mqtt"
)

// Key ...
const Key = "ping"

// Stats ...
type Stats struct {
	Sent        int64   `json:"sent"`
	Received    int64   `json:"received"`
	Count       int64   `json:"-"`
	LossNow     float64 `json:"loss_now"`
	Loss24Hours float64 `json:"loss_24_hours"`
}

// Message ...
type Message struct {
	Version   int64     `json:"version"`
	Timestamp time.Time `json:"timestamp"`

	Internet struct {
		Ping Stats `json:"ping"`
		DNS  Stats `json:"dns"`
	} `json:"internet"`

	Gateway struct {
		Ping Stats `json:"ping"`
	} `json:"gateway"`

	Tunnel struct {
		Ping Stats `json:"ping"`
	} `json:"tunnel"`
}

// Handler ...
type Handler struct {
	broker     *mqtt.Broker
	formations *devices.FormationMap
}

// Register ...
func Register(broker *mqtt.Broker, formations *devices.FormationMap) interface{} {
	h := &Handler{broker, formations}

	broker.Subscribe("/pylon/+/wan/ping", h.onMessage)
	return h
}

func (h *Handler) onMessage(topic string, payload interface{}) error {
	buf, ok := payload.([]byte)
	if !ok {
		return fmt.Errorf("[ping] expected byte buffer, got this instead: %v", payload)
	}

	msg := new(Message)
	if err := json.Unmarshal(buf, msg); err != nil {
		return err
	}

	deviceName := devices.ParseTopic(topic).DeviceName
	var currentState *Message
	rawState := h.formations.GetDeviceState(deviceName, Key)
	if rawState != nil {
		currentState = rawState.(*Message)
	}

	newState := updatePingState(currentState, msg)
	formationID := h.formations.FormationID(deviceName)
	h.formations.PutDeviceState(formationID, deviceName, Key, newState)
	h.broker.Publish(fmt.Sprintf("/armada/%s/wan/ping", deviceName), newState)
	return nil
}

func updatePingState(currentState, msg *Message) *Message {
	if currentState == nil {
		currentState = msg
	}

	resetCount := false
	nowUTC := time.Now().UTC()
	// Check if the last timestamp is older than 12 hours
	if nowUTC.Sub(currentState.Timestamp) >= 12*time.Hour {
		resetCount = true
		currentState.Timestamp = nowUTC
	}

	UpdateLosses(&currentState.Internet.Ping, msg.Internet.Ping.Sent, msg.Internet.Ping.Received, resetCount)
	UpdateLosses(&currentState.Internet.DNS, msg.Internet.DNS.Sent, msg.Internet.DNS.Received, resetCount)
	UpdateLosses(&currentState.Gateway.Ping, msg.Gateway.Ping.Sent, msg.Gateway.Ping.Received, resetCount)
	UpdateLosses(&currentState.Tunnel.Ping, msg.Tunnel.Ping.Sent, msg.Tunnel.Ping.Received, resetCount)

	return currentState
}

// UpdateLosses mutates members of the first parameter
func UpdateLosses(stats *Stats, sent, received int64, resetCount bool) {
	if received == 0 {
		stats.LossNow = 1.0
	} else {
		stats.LossNow = devices.Round(1.0-float64(received)/float64(sent), 2)
	}

	stats.Loss24Hours = (stats.Loss24Hours*float64(stats.Count) + stats.LossNow) / float64(stats.Count+1)
	stats.Loss24Hours = devices.Round(stats.Loss24Hours, 2)

	stats.Count++

	if resetCount {
		stats.Count = stats.Count / 2
		if stats.Count < 1000 {
			stats.Count = 1000
		}
	}

	stats.Sent = sent
	stats.Received = received
}

// MarshalJSON ...
func (m *Message) MarshalJSON() ([]byte, error) {
	type Alias Message
	return json.Marshal(&struct {
		*Alias
		Timestamp int64 `json:"timestamp"`
	}{
		Alias:     (*Alias)(m),
		Timestamp: m.Timestamp.Unix(),
	})
}

// UnmarshalJSON ...
func (m *Message) UnmarshalJSON(data []byte) error {
	type Alias Message
	tmp := &struct {
		Timestamp int64 `json:"timestamp"`
		*Alias
	}{
		Alias: (*Alias)(m),
	}
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}
	m.Timestamp = time.Unix(tmp.Timestamp, 0)
	return nil
}
