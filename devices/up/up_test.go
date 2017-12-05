package up_test

import (
	"encoding/json"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/superscale/spire/devices"
	"github.com/superscale/spire/devices/up"
	"github.com/superscale/spire/mqtt"
	"github.com/superscale/spire/testutils"
)

var _ = Describe("Up Message Handler", func() {

	var broker *mqtt.Broker
	var formations *devices.FormationMap
	var recorder *testutils.PubSubRecorder

	var formationID = "00000000-0000-0000-0000-000000000001"
	var deviceName = "1.marsara"
	var upTopic = "matriarch/1.marsara/up"

	BeforeEach(func() {
		broker = mqtt.NewBroker(false)
		formations = devices.NewFormationMap()
		recorder = testutils.NewPubSubRecorder()

		broker.Subscribe(upTopic, recorder)
		up.Register(broker, formations)
	})
	Describe("connect", func() {
		BeforeEach(func() {
			m := devices.ConnectMessage{FormationID: formationID, DeviceName: deviceName, DeviceInfo: nil}
			broker.Publish(devices.ConnectTopic.String(), m)
		})
		It("publishes an 'up' message for the device with state = \"up\"", func() {
			Eventually(func() int {
				return recorder.Count()
			}).Should(BeNumerically("==", 1))

			topic, raw := recorder.First()

			Expect(topic).To(Equal(upTopic))

			msg, ok := raw.(up.Message)
			Expect(ok).To(BeTrue())

			Expect(msg.State).To(Equal(up.Up))
			Expect(msg.Timestamp).To(BeNumerically(">", 0))
			Expect(msg.Timestamp).To(BeNumerically("<=", time.Now().UTC().Unix()))
		})
		It("stores the 'up' state of the device", func() {
			formations.RLock()
			defer formations.RUnlock()

			msg, ok := formations.GetDeviceState(deviceName, up.Key).(up.Message)
			Expect(ok).To(BeTrue())

			Expect(msg.State).To(Equal(up.Up))
			Expect(msg.Timestamp).To(BeNumerically(">", 0))
			Expect(msg.Timestamp).To(BeNumerically("<=", time.Now().UTC().Unix()))
		})
		Describe("disconnect", func() {
			var connectedSince int64

			BeforeEach(func() {
				connectedSince = time.Now().Add(-time.Minute * 5).Unix()

				msg := up.Message{
					State:     up.Up,
					Timestamp: connectedSince,
				}

				formations.Lock()
				formations.PutDeviceState(formationID, deviceName, up.Key, msg)
				formations.Unlock()

				m := devices.DisconnectMessage{FormationID: formationID, DeviceName: deviceName}
				broker.Publish(devices.DisconnectTopic.String(), m)
			})
			It("publishes an 'up' message for the device with state = \"down\"", func() {
				Eventually(func() int {
					return recorder.Count()
				}).Should(BeNumerically("==", 2))

				topic, raw := recorder.Last()
				Expect(topic).To(Equal(upTopic))

				msg, ok := raw.(up.Message)
				Expect(ok).To(BeTrue())

				Expect(msg.State).To(Equal(up.Down))
				Expect(msg.Timestamp).To(BeNumerically("<=", time.Now().UTC().Unix()))
				Expect(msg.Timestamp).To(BeNumerically(">", connectedSince))
			})
			It("updates the device state", func() {
				formations.RLock()
				defer formations.RUnlock()

				msg, ok := formations.GetDeviceState(deviceName, up.Key).(up.Message)
				Expect(ok).To(BeTrue())

				Expect(msg.State).To(Equal(up.Down))
				Expect(msg.Timestamp).To(BeNumerically("<=", time.Now().UTC().Unix()))
				Expect(msg.Timestamp).To(BeNumerically(">", connectedSince))
			})
		})
	})
	Describe("sends current state on subscribe", func() {
		var brokerSession, subscriberSession *mqtt.Session
		var message up.Message

		JustBeforeEach(func() {
			brokerSession, subscriberSession = testutils.Pipe()

			subPkg := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
			subPkg.Topics = []string{"matriarch/1.marsara/#"}
			subPkg.MessageID = 1337

			go broker.HandleSubscribePacket(subPkg, brokerSession, true)

			// read suback packet
			_, err := subscriberSession.Read()
			Expect(err).NotTo(HaveOccurred())

			p, err := subscriberSession.Read()
			Expect(err).NotTo(HaveOccurred())

			pubPkg, ok := p.(*packets.PublishPacket)
			Expect(ok).To(BeTrue())

			err = json.Unmarshal(pubPkg.Payload, &message)
			Expect(err).NotTo(HaveOccurred())
		})
		AfterEach(func() {
			brokerSession.Close()
			subscriberSession.Close()
		})
		Context("with no device state in the cache", func() {
			It("publishes an 'up' message for the device with state = \"down\"", func() {
				Expect(message.State).To(Equal(up.Down))
				Expect(message.Timestamp).To(Equal(int64(0)))
			})
		})
		Context("with device state in the cache", func() {
			var connectedSince int64

			BeforeEach(func() {
				formations.Lock()
				defer formations.Unlock()

				connectedSince = time.Now().Add(-time.Minute * 5).UTC().Unix()

				msg := up.Message{
					State:     up.Up,
					Timestamp: connectedSince,
				}

				formations.PutDeviceState(formationID, deviceName, up.Key, msg)
			})
			It("publishes an 'up' message for the device with state = \"up\"", func() {
				Expect(message.State).To(Equal(up.Up))
				Expect(message.Timestamp).To(Equal(connectedSince))
			})
		})
	})
})
