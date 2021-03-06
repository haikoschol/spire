package testutils

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/superscale/spire/mqtt"
)

// Pipe ...
func Pipe() (*mqtt.Session, *mqtt.Session) {
	a, b := net.Pipe()
	t := time.Second * 1
	return mqtt.NewSession(a, t), mqtt.NewSession(b, t)
}

// PubSubRecorder ...
type PubSubRecorder struct {
	Topics   []string
	Messages []interface{}
	l        sync.RWMutex
}

// NewPubSubRecorder ...
func NewPubSubRecorder() *PubSubRecorder {
	return &PubSubRecorder{
		Topics:   []string{},
		Messages: []interface{}{},
	}
}

// HandleMessage implements mqtt.Subscriber
func (r *PubSubRecorder) HandleMessage(topic string, payload interface{}) error {
	r.l.Lock()
	defer r.l.Unlock()

	r.Topics = append(r.Topics, topic)
	r.Messages = append(r.Messages, payload)
	return nil
}

// Count ...
func (r *PubSubRecorder) Count() int {
	r.l.RLock()
	defer r.l.RUnlock()

	return len(r.Topics)
}

// Get ...
func (r *PubSubRecorder) Get(i int) (string, interface{}) {
	r.l.RLock()
	defer r.l.RUnlock()

	if i < len(r.Topics) && i < len(r.Messages) {
		return r.Topics[i], r.Messages[i]
	}

	return "", nil
}

// First ...
func (r *PubSubRecorder) First() (string, interface{}) {
	return r.Get(0)
}

// Last ...
func (r *PubSubRecorder) Last() (string, interface{}) {
	return r.Get(r.Count() - 1)
}

// WriteConnectPacket ...
func WriteConnectPacket(formationID, deviceName, ipAddress string, session *mqtt.Session) error {
	pkg := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)

	pkg.ClientIdentifier = deviceName
	pkg.UsernameFlag = true
	pkg.Username = fmt.Sprintf(`{"formation_id": "%s", "ip_address": "%s"}`, formationID, ipAddress)

	return session.Write(pkg)
}
