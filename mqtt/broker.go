package mqtt

import (
	"fmt"
	"io"
	"log"
	"strings"
	"sync"

	bugsnag "github.com/bugsnag/bugsnag-go"
	bugsnagErrors "github.com/bugsnag/bugsnag-go/errors"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/superscale/spire/monitoring"
)

// InternalTopicPrefix Topics with this prefix are reserved for internal use.
// Publish packets with these topics will be ignored.
const InternalTopicPrefix = "$SYS"

// Subscriber ...
type Subscriber interface {
	// HandleMessage ...
	HandleMessage(topic string, message interface{}) error
}

type subscriberMap map[string][]Subscriber

// SubscribeEventTopic is used by the broker to publish subscribe events on.
const SubscribeEventTopic = InternalTopicPrefix + "/subscribe"

// SubscribeMessage is the payload sent on SubscribeEventTopic when the broker
// receives a subscribe packet.
type SubscribeMessage struct {
	Topics []string
}

// Broker manages pub/sub
type Broker struct {
	l           sync.RWMutex
	subscribers subscriberMap
	topicPrefix bool
}

// NewBroker ...
// If topicPrefix is true, Subscribe() and Publish() will add a leading slash to topics that
// don't have one.
func NewBroker(topicPrefix bool) *Broker {
	return &Broker{
		subscribers: make(subscriberMap),
		topicPrefix: topicPrefix,
	}
}

// HandleConnection ...
func (b *Broker) HandleConnection(session *Session) {
	if _, err := session.Handshake(); err != nil {
		if err != io.EOF {
			log.Println(err)
		}
		return
	}
	monitoring.AddControlClient()

	for {
		pkg, err := session.Read()
		if err != nil {
			if err != io.EOF {
				log.Println(err)
				session.Close()
			}
			b.Remove(session)
			return
		}

		switch p := pkg.(type) {
		case *packets.PingreqPacket:
			err = session.SendPingresp()
		case *packets.PublishPacket:
			if !strings.HasPrefix(p.TopicName, InternalTopicPrefix+"/") {
				b.Publish(p.TopicName, p.Payload)
			}
		case *packets.SubscribePacket:
			err = b.HandleSubscribePacket(p, session, true)
		case *packets.UnsubscribePacket:
			b.UnsubscribeAll(p, session)
			err = session.SendUnsuback(p.MessageID)
		default:
			b.Remove(session)
			if err = session.Close(); err != nil {
				log.Println(err)
			}
			return
		}

		if err != nil {
			log.Printf("error while handling packet in broker. peer %v: %v", session.RemoteAddr(), err)
		}
	}
}

// Subscribe ...
func (b *Broker) Subscribe(topic string, s Subscriber) {
	if len(topic) == 0 {
		return
	}
	topic = b.normalizeTopic(topic)

	b.l.Lock()
	defer b.l.Unlock()

	b.subscribe(topic, s)
}

func (b *Broker) subscribe(topic string, s Subscriber) {
	subs, exists := b.subscribers[topic]
	if !exists {
		b.subscribers[topic] = []Subscriber{s}
		return
	}

	if indexOf(subs, s) != -1 {
		return
	}

	b.subscribers[topic] = append(subs, s)
}

// HandleSubscribePacket subscribes the peer to all topics included in the packet
// and publishes a SubscribeMessage under SubscribeEventTopic if sendSubscribeMessage is true.
func (b *Broker) HandleSubscribePacket(pkg *packets.SubscribePacket, session *Session, sendSubscribeMessage bool) error {
	b.l.Lock()

	for _, topic := range pkg.Topics {
		b.subscribe(topic, session)
	}
	if err := session.SendSuback(pkg.MessageID); err != nil {
		for _, topic := range pkg.Topics {
			b.unsubscribe(topic, session)
		}
		b.l.Unlock()
		return err
	}
	b.l.Unlock()

	if sendSubscribeMessage {
		subscribeMessage := SubscribeMessage{Topics: make([]string, len(pkg.Topics))}
		copy(subscribeMessage.Topics, pkg.Topics)
		b.Publish(SubscribeEventTopic, subscribeMessage)
	}
	return nil
}

// Unsubscribe ...
func (b *Broker) Unsubscribe(topic string, s Subscriber) {
	if len(topic) == 0 {
		return
	}
	topic = b.normalizeTopic(topic)

	b.l.Lock()
	defer b.l.Unlock()

	b.unsubscribe(topic, s)
}

func (b *Broker) unsubscribe(topic string, s Subscriber) {
	subs, exists := b.subscribers[topic]
	if !exists {
		return
	}

	i := indexOf(subs, s)
	if i < 0 {
		return
	}

	// from https://github.com/golang/go/wiki/SliceTricks
	copy(subs[i:], subs[i+1:])
	subs[len(subs)-1] = nil
	subs = subs[:len(subs)-1]

	if len(subs) == 0 {
		delete(b.subscribers, topic)
	} else {
		b.subscribers[topic] = subs
	}
}

// UnsubscribeAll ...
func (b *Broker) UnsubscribeAll(pkg *packets.UnsubscribePacket, s Subscriber) {
	b.l.Lock()
	defer b.l.Unlock()

	for _, topic := range pkg.Topics {
		b.unsubscribe(topic, s)
	}
}

// Publish ...
func (b *Broker) Publish(topic string, message interface{}) {
	if len(topic) == 0 {
		return
	}
	topic = b.normalizeTopic(topic)

	b.l.RLock()
	defer b.l.RUnlock()

	topics := MatchTopics(topic, b.topics())
	if len(topics) == 0 {
		return
	}

	subs := []Subscriber{}

	for _, t := range topics {
		subs = append(subs, b.get(t)...)
	}

	for _, s := range subs {
		if err := s.HandleMessage(topic, message); err != nil {
			log.Println(err)

			if _, ok := err.(*bugsnagErrors.Error); ok {
				notifyBugsnag(err, "spire:publish", bugsnag.MetaData{"Publish": {"Topic": topic}})
			}
		}
	}
}

// Remove ...
func (b *Broker) Remove(s Subscriber) {
	monitoring.RemoveControlClient()

	b.l.Lock()
	defer b.l.Unlock()

	for topic := range b.subscribers {
		b.unsubscribe(topic, s)
	}
}

// MatchTopics returns the subset of "topics" that matches "topic".
// assumes that "topic" does not contain wildcards
func MatchTopics(topic string, topics []string) []string {
	matches := []string{}
	topicParts := strings.Split(topic, "/")

	for _, t := range topics {
		if TopicsMatch(topicParts, strings.Split(t, "/")) {
			matches = append(matches, t)
		}
	}
	return matches
}

func (b *Broker) topics() []string {
	i := 0
	res := make([]string, len(b.subscribers))
	for topic := range b.subscribers {
		res[i] = topic
		i++
	}

	return res
}

func (b *Broker) get(topic string) []Subscriber {
	subs, exists := b.subscribers[topic]
	if !exists {
		return []Subscriber{}
	}
	return subs
}

const singleLevelWildcard = "+"
const multiLevelWildcard = "#"

// TopicsMatch returns true if t1 matches t2 as specified in MQTT
// parameters are the topics split on "/"
// assumes that the topic in the first parameter does not contain wildcards
// returns false for invalid topics (multi-level wildcards somewhere other than at the end)
func TopicsMatch(t1, t2 []string) bool {
	l1 := len(t1)
	l2 := len(t2)

	if l1 != l2 && t2[l2-1] != multiLevelWildcard {
		return false
	}

	l := l1
	if l2 < l1 {
		l = l2
	}

	for i := 0; i < l; i++ {
		if t1[i] != t2[i] {

			if t2[i] == singleLevelWildcard {
				continue
			}

			if t2[i] == multiLevelWildcard {
				return i+1 == len(t2)
			}

			return false
		}
	}
	return true
}

func (b *Broker) normalizeTopic(topic string) string {
	if b.topicPrefix && topic[0] != '/' {
		return fmt.Sprintf("/%s", topic)
	}
	return topic
}

func indexOf(subscribers []Subscriber, s Subscriber) int {
	for i, sub := range subscribers {
		if sub == s {
			return i
		}
	}
	return -1
}
