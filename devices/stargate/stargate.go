package stargate

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	bugsnagErrors "github.com/bugsnag/bugsnag-go/errors"
	"github.com/superscale/spire/devices"
	"github.com/superscale/spire/mqtt"
)

type endpoint struct {
	IP   *string `json:"ip"`
	Port *int    `json:"port"`
}

// http://imgur.com/qni0caW
type assimilator struct {
	Message struct {
		Assimilator interface{} `json:"assimilator"`
	} `json:"message"`
	From endpoint `json:"from"`
}

// PortsMessage ...
type PortsMessage struct {
	TFTPD struct {
		Client       endpoint `json:"client"`
		Listening    *bool    `json:"listening"`
		Request      *string  `json:"request"`
		Total        *int64   `json:"total"`
		Progress     *int64   `json:"progress"`
		Error        *string  `json:"error"`
		Transmitting *int64   `json:"transmitting"`
		Finished     bool     `json:"finished"`
	} `json:"tftpd"`

	Assimilator *assimilator `json:"assimilator"`
	Request     *string      `json:"request"`
	Port        int          `json:"port"`
	Up          interface{}  `json:"up"`
}

const (
	// Key ...
	Key = "stargate"
	// Wait ...
	Wait = "wait"
	// API ...
	API = "api"
	// Start ...
	Start = "start"
	// Transmitting ...
	Transmitting = "transmitting"
	// Progress ...
	Progress = "progress"
	// Flashing ...
	Flashing = "flashing"
	// Assimilator ...
	Assimilator = "assimilator"
	// Ok ...
	Ok = "ok"
	// Error ...
	Error = "error"
)

// PortState ...
type PortState struct {
	Timestamp   int64       `json:"timestamp"`
	State       string      `json:"state"`
	File        string      `json:"file,omitempty"`
	Total       int64       `json:"total,omitempty"`
	Progress    int64       `json:"progress,omitempty"`
	Error       string      `json:"error,omitempty"`
	Assimilator interface{} `json:"assimilator,omitempty"`
}

// NewPortState ...
func NewPortState() *PortState {
	return &PortState{
		Timestamp: time.Now().UTC().Unix(),
		State:     Wait,
	}
}

// PortMap ...
type PortMap map[int]*PortState

// SystemImageMessage ...
type SystemImageMessage struct {
	ID       string `json:"id"`
	Download string `json:"download"`
	Vendor   string `json:"vendor"`
	Product  string `json:"product"`
	Total    int64  `json:"total"`
	Progress int64  `json:"progress"`
}

// SystemImageState ...
type SystemImageState struct {
	ID       string `json:"id"`
	State    string `json:"state"`
	Vendor   string `json:"vendor"`
	Product  string `json:"product"`
	Total    int64  `json:"-"`
	Progress int64  `json:"progress,omitempty"`
}

// NewSystemImageState ...
func NewSystemImageState(id, vendor, product string) *SystemImageState {
	return &SystemImageState{
		ID:      id,
		State:   Start,
		Vendor:  vendor,
		Product: product,
	}
}

// SystemImageMap ...
type SystemImageMap map[string]*SystemImageState

// State ...
type State struct {
	Ports        PortMap
	SystemImages SystemImageMap
}

// NewState ...
func NewState() *State {
	return &State{
		Ports:        make(PortMap),
		SystemImages: make(SystemImageMap),
	}
}

// Handler ...
type Handler struct {
	broker     *mqtt.Broker
	formations *devices.FormationMap
}

// Register ...
func Register(broker *mqtt.Broker, formations *devices.FormationMap) interface{} {
	h := &Handler{
		broker:     broker,
		formations: formations,
	}

	broker.Subscribe("pylon/+/stargate/port", h)
	broker.Subscribe("pylon/+/stargate/systemimaged", h)
	return h
}

// HandleMessage ...
func (h *Handler) HandleMessage(topic string, message interface{}) error {
	h.formations.Lock()
	defer h.formations.Unlock()

	t := devices.ParseTopic(topic)

	switch t.Path {
	case "stargate/port":
		msg, err := unmarshalPortsMessage(message)
		if err != nil {
			return err
		}
		return h.onPortsMessage(t, msg)
	case "stargate/systemimaged":
		msg, err := unmarshalSystemImageMessage(message)
		if err != nil {
			return err
		}
		return h.onSystemImageMessage(t, msg)
	default:
		return nil
	}
}

func (h *Handler) onPortsMessage(t devices.Topic, msg *PortsMessage) error {
	state := h.getState(t.DeviceName)

	if msg.Up != nil || (msg.TFTPD.Listening != nil && *msg.TFTPD.Listening == true) {
		h.handleUp(t.DeviceName, state, msg.Port)
	} else {
		ps := h.getPortState(state, t.DeviceName, msg.Port)

		if msg.TFTPD.Request != nil && msg.TFTPD.Total != nil {
			ps.File = *msg.TFTPD.Request
			ps.Total = *msg.TFTPD.Total
			ps.State = Start

		} else if msg.TFTPD.Transmitting != nil {
			progress := *msg.TFTPD.Transmitting
			ps.Progress = int64(devices.Round(float64(progress)/float64(ps.Total)*100.0, 0))
			ps.State = Transmitting

		} else if msg.TFTPD.Finished == true {
			ps.Progress = 100.0
			ps.State = Flashing

		} else if msg.TFTPD.Error != nil {
			ps.Error = *msg.TFTPD.Error
			ps.State = Error

		} else if msg.Assimilator != nil {
			ps.Assimilator = (*msg.Assimilator).Message.Assimilator
			ps.State = Assimilator
		}
	}

	h.formations.PutDeviceState(h.formations.FormationID(t.DeviceName), t.DeviceName, Key, state)
	h.broker.Publish(fmt.Sprintf("matriarch/%s/stargate/ports", t.DeviceName), state.Ports)
	return nil
}

func (h *Handler) handleUp(deviceName string, state *State, port int) {
	ps, exists := state.Ports[port]
	if exists {
		ps.State = Wait
		ps.Timestamp = time.Now().UTC().Unix()
	} else {
		state.Ports[port] = NewPortState()
	}
}

func (h *Handler) getState(deviceName string) *State {
	state, ok := h.formations.GetDeviceState(deviceName, Key).(*State)
	if !ok {
		return NewState()
	}
	return state
}

func (h *Handler) getPortState(state *State, deviceName string, port int) *PortState {
	ps, exists := state.Ports[port]
	if !exists {
		log.Printf("[stargate] warning: expected state for port %d on device %s missing", port, deviceName)
		ps = NewPortState()
		state.Ports[port] = ps
	} else {
		ps.Timestamp = time.Now().UTC().Unix()
	}
	return ps
}

func (h *Handler) onSystemImageMessage(t devices.Topic, msg *SystemImageMessage) error {
	state := h.getState(t.DeviceName)

	if msg.Download == API {
		state.SystemImages[msg.ID] = NewSystemImageState(msg.ID, msg.Vendor, msg.Product)
	} else {
		imgState, exists := state.SystemImages[msg.ID]
		if !exists {
			return bugsnagErrors.New(fmt.Errorf("missing system image state for image %s on device %s", msg.ID, t.DeviceName), 1)
		}

		switch msg.Download {
		case Start:
			imgState.Total = msg.Total
		case Progress:
			imgState.Progress = int64(devices.Round(float64(msg.Progress)/float64(imgState.Total)*100.0, 0))
		case Ok:
			imgState.State = Ok
			imgState.Progress = 100
		default:
			imgState.State = Error
		}
	}

	h.formations.PutDeviceState(h.formations.FormationID(t.DeviceName), t.DeviceName, Key, state)
	h.broker.Publish(fmt.Sprintf("matriarch/%s/stargate/system_images", t.DeviceName), state.SystemImages)
	return nil
}

func unmarshalPortsMessage(message interface{}) (*PortsMessage, error) {
	buf, ok := message.([]byte)
	if !ok {
		return nil, fmt.Errorf("[stargate] expected byte buffer, got this instead: %v", message)
	}

	msg := new(PortsMessage)
	if err := json.Unmarshal(buf, msg); err != nil {
		return nil, bugsnagErrors.New(err, 1)
	}
	return msg, nil
}

func unmarshalSystemImageMessage(message interface{}) (*SystemImageMessage, error) {
	buf, ok := message.([]byte)
	if !ok {
		return nil, fmt.Errorf("[stargate] expected byte buffer, got this instead: %v", message)
	}

	msg := new(SystemImageMessage)
	if err := json.Unmarshal(buf, msg); err != nil {
		return nil, bugsnagErrors.New(err, 1)
	}
	return msg, nil
}
