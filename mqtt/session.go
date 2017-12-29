package mqtt

import (
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/superscale/spire/monitoring"
)

// Session represents an MQTT connection
type Session struct {
	conn        net.Conn
	idleTimeout time.Duration
}

// NewSession returns a new mqtt.Session
func NewSession(conn net.Conn, idleTimeout time.Duration) *Session {
	return &Session{
		conn:        conn,
		idleTimeout: idleTimeout,
	}
}

// ReadConnect reads the connect packet or times out
func (s *Session) ReadConnect() (p *packets.ConnectPacket, err error) {
	s.conn.SetReadDeadline(s.deadline())

	var ca packets.ControlPacket
	if ca, err = packets.ReadPacket(s.conn); err != nil {
		return
	}

	var ok bool
	if p, ok = ca.(*packets.ConnectPacket); !ok {
		return nil, fmt.Errorf("expected a CONNECT packet from %v, got this instead: %s", s.conn.RemoteAddr(), ca.String())
	}

	return
}

// AcknowledgeConnect ...
func (s *Session) AcknowledgeConnect() error {
	cAck := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
	s.conn.SetWriteDeadline(s.deadline())
	return cAck.Write(s.conn)
}

// Handshake performs the connection handshake and returns the connect packet or an error
func (s *Session) Handshake() (p *packets.ConnectPacket, err error) {
	if p, err = s.ReadConnect(); err != nil {
		return
	}

	if err = s.AcknowledgeConnect(); err != nil {
		p = nil
	}
	return
}

// Close ...
func (s *Session) Close() error {
	return s.conn.Close()
}

// RemoteAddr ...
func (s *Session) RemoteAddr() net.Addr {
	return s.conn.RemoteAddr()
}

// Read a packet or time out
func (s *Session) Read() (pkg packets.ControlPacket, err error) {
	s.conn.SetReadDeadline(s.deadline())
	pkg, err = packets.ReadPacket(s.conn)

	if p, ok := pkg.(*packets.PublishPacket); ok {
		monitoring.CountMessageIngress(p.TopicName)
	}
	return
}

// Write a packet or time out
func (s *Session) Write(pkg packets.ControlPacket) error {
	if p, ok := pkg.(*packets.PublishPacket); ok {
		monitoring.CountMessageEgress(p.TopicName)
	}

	s.conn.SetWriteDeadline(s.deadline())
	return pkg.Write(s.conn)
}

// SendPingresp ...
func (s *Session) SendPingresp() error {
	resp := packets.NewControlPacket(packets.Pingresp).(*packets.PingrespPacket)
	return s.Write(resp)
}

// HandleMessage serializes the message to JSON (unless it is a []byte)
// and sends a PUBLISH packet with QoS 0
func (s *Session) HandleMessage(topic string, message interface{}) error {
	var payload []byte
	var ok bool
	var err error

	payload, ok = message.([]byte)
	if !ok {
		payload, err = json.Marshal(message)
		if err != nil {
			return err
		}
	}

	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.Qos = 0
	p.TopicName = topic
	p.Payload = payload

	return s.Write(p)
}

// SendSuback ...
func (s *Session) SendSuback(messageID uint16) error {
	sAck := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
	sAck.MessageID = messageID
	return s.Write(sAck)
}

// SendUnsuback ...
func (s *Session) SendUnsuback(messageID uint16) error {
	sAck := packets.NewControlPacket(packets.Unsuback).(*packets.UnsubackPacket)
	sAck.MessageID = messageID
	return s.Write(sAck)
}

func (s *Session) deadline() time.Time {
	return time.Now().UTC().Add(s.idleTimeout)
}
