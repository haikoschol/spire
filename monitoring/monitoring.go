package monitoring

import (
	"log"
	"sync/atomic"
	"time"

	"github.com/DataDog/datadog-go/statsd"
)

const (
	deviceClientsID     = "clients.device"
	controlClientsID    = "clients.control"
	msgIngressID        = "messages.ingress"
	msgEgressID         = "messages.egress"
	deviceInfoRequestID = "requests.device_info"
)

var (
	deviceClients  int64
	controlClients int64
	client         *statsd.Client
	t              = make([]string, 0)
)

// InitMonitoring must be called before any of the other functions in this package, to enable data being sent to
// the statsd instance listening on addr. It does nothing if addr is an empty string or when called a second time.
func InitMonitoring(addr string) (err error) {
	if client != nil {
		return
	}

	if client, err = statsd.New(addr); err == nil {
		client.Namespace = "spire."
	}
	return
}

// AddDeviceClient increases the number of connected device clients
func AddDeviceClient() {
	if client == nil {
		return
	}

	atomic.AddInt64(&deviceClients, 1)
	gauge(deviceClientsID, deviceClients)
}

// RemoveDeviceClient decreases the number of connected device clients
func RemoveDeviceClient() {
	if client == nil {
		return
	}

	atomic.AddInt64(&deviceClients, -1)
	gauge(deviceClientsID, deviceClients)
}

// AddControlClient increases the number of connected control clients
func AddControlClient() {
	if client == nil {
		return
	}

	atomic.AddInt64(&controlClients, 1)
	gauge(controlClientsID, controlClients)
}

// RemoveControlClient decreases the number of connected control clients
func RemoveControlClient() {
	if client == nil {
		return
	}

	atomic.AddInt64(&controlClients, -1)
	gauge(controlClientsID, controlClients)
}

// CountMessageIngress increments the counter for messages received over the network
func CountMessageIngress(topic string) {
	if client == nil {
		return
	}

	count(msgIngressID, topic)
}

// CountMessageEgress increments the counter for messages sent over the network
func CountMessageEgress(topic string) {
	if client == nil {
		return
	}

	count(msgEgressID, topic)
}

// Segment records timing information for a segment of code
type Segment struct {
	startTime time.Time
	name      string
	tag       string
}

// End calculates the duration of the timed code segment and records the data
func (s *Segment) End() {
	if client == nil {
		return
	}

	duration := time.Now().UTC().Sub(s.startTime)
	millis := float64(duration.Nanoseconds() / 1000)

	if err := client.TimeInMilliseconds(s.name, millis, []string{s.tag}, 1.0); err != nil {
		log.Print(err)
	}
}

// StartDeviceInfoSegment starts the timer for a GET liberator/v2/devices/<name> request
func StartDeviceInfoSegment(deviceName string) *Segment {
	return &Segment{
		startTime: time.Now().UTC(),
		name:      deviceInfoRequestID,
		tag:       "device_name:" + deviceName,
	}
}

func gauge(name string, value int64) {
	if err := client.Gauge(name, float64(value), t, 1); err != nil {
		log.Print(err)
	}
}

func count(name, topic string) {
	if err := client.Count(name, 1, []string{"topic:" + topic}, 1); err != nil {
		log.Print(err)
	}
}
