package ping_test

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/superscale/spire/devices"
	"github.com/superscale/spire/devices/ping"
	"github.com/superscale/spire/mqtt"
	"github.com/superscale/spire/testutils"
)

var _ = Describe("Ping Message Handler", func() {

	var broker *mqtt.Broker
	var formations *devices.FormationMap
	var recorder *testutils.PubSubRecorder

	var deviceName = "1.marsara"
	var deviceTopic = "/pylon/1.marsara/wan/ping"
	var controlTopic = "/armada/1.marsara/wan/ping"
	var payload []byte

	BeforeEach(func() {
		broker = mqtt.NewBroker()
		formations = devices.NewFormationMap()
		recorder = testutils.NewPubSubRecorder()

		broker.Subscribe(controlTopic, recorder.Record)
		ping.Register(broker, formations)
	})
	Context("first ping message from this device", func() {
		var firstPingTimestamp time.Time

		BeforeEach(func() {
			pingState := formations.GetDeviceState(deviceName, ping.Key)
			Expect(pingState).To(BeNil())

			firstPingTimestamp = time.Now().Add(-10 * time.Minute)
			payload = []byte(fmt.Sprintf(`
				{
					"version": 1,
					"timestamp": %d,
					"gateway": {
						"ping": {
							"received": 1,
							"sent": 1
						}
					},
					"internet": {
						"ping": {
							"received": 1,
							"sent": 1
						},
						"dns": {
							"received": 1,
							"sent": 1
						}
					},
					"tunnel": {
						"ping": {
							"received": 1,
							"sent": 1
						}
					}
				}
			`, firstPingTimestamp.Unix()))

			broker.Publish(deviceTopic, payload)
		})
		It("adds ping state to the device state", func() {
			state := formations.GetDeviceState(deviceName, ping.Key)
			_, ok := state.(*ping.Message)
			Expect(ok).To(BeTrue())
		})
		It("initializes ping state with the values from the message", func() {
			state := formations.GetDeviceState(deviceName, ping.Key)
			ps, ok := state.(*ping.Message)
			Expect(ok).To(BeTrue())

			var e int64 = 1
			Expect(ps.Version).To(Equal(e))

			// Unix() limits precision to seconds
			Expect(ps.Timestamp.Unix()).To(Equal(firstPingTimestamp.Unix()))

			Expect(ps.Internet.Ping.Sent).To(Equal(e))
			Expect(ps.Internet.Ping.Received).To(Equal(e))

			Expect(ps.Internet.DNS.Sent).To(Equal(e))
			Expect(ps.Internet.DNS.Received).To(Equal(e))

			Expect(ps.Gateway.Ping.Sent).To(Equal(e))
			Expect(ps.Gateway.Ping.Received).To(Equal(e))

			Expect(ps.Tunnel.Ping.Sent).To(Equal(e))
			Expect(ps.Tunnel.Ping.Received).To(Equal(e))
		})
		It("publishes the ping message", func() {
			Expect(recorder.Count()).To(BeNumerically("==", 1))

			topic, raw := recorder.First()
			Expect(topic).To(Equal(controlTopic))

			m, ok := raw.(*ping.Message)
			Expect(ok).To(BeTrue())

			var e int64 = 1
			Expect(m.Version).To(Equal(e))
			Expect(m.Timestamp.Unix()).To(Equal(firstPingTimestamp.Unix()))

			Expect(m.Internet.Ping.Sent).To(Equal(e))
			Expect(m.Internet.Ping.Received).To(Equal(e))

			Expect(m.Internet.DNS.Sent).To(Equal(e))
			Expect(m.Internet.DNS.Received).To(Equal(e))

			Expect(m.Gateway.Ping.Sent).To(Equal(e))
			Expect(m.Gateway.Ping.Received).To(Equal(e))

			Expect(m.Tunnel.Ping.Sent).To(Equal(e))
			Expect(m.Tunnel.Ping.Received).To(Equal(e))
		})
		Context("subsequent ping message from this device", func() {
			JustBeforeEach(func() {
				broker.Publish(deviceTopic, payload)

				pl := `
				{
					"version": 1,
					"timestamp": %d,
					"gateway": {
						"ping": {
							"received": 2,
							"sent": 3
						}
					},
					"internet": {
						"ping": {
							"received": 2,
							"sent": 3
						},
						"dns": {
							"received": 2,
							"sent": 3
						}
					},
					"tunnel": {
						"ping": {
							"received": 2,
							"sent": 3
						}
					}
				}`

				ts := firstPingTimestamp
				for i := 0; i < 50; i++ {
					ts = ts.Add(10 * time.Second)
					broker.Publish(deviceTopic, []byte(fmt.Sprintf(pl, ts.Unix())))
				}
			})
			It("keeps counts and calculates losses", func() {
				state := formations.GetDeviceState(deviceName, ping.Key)
				ps, ok := state.(*ping.Message)
				Expect(ok).To(BeTrue())

				Expect(ps.Timestamp.Unix()).To(Equal(firstPingTimestamp.Unix()))
				Ω(ps.Internet.Ping.Count).Should(BeNumerically("==", 52))
				Ω(ps.Internet.Ping.LossNow).Should(BeNumerically(">", 0.32))
				Ω(ps.Internet.Ping.LossNow).Should(BeNumerically("<", 0.34))

				Ω(ps.Internet.DNS.Count).Should(BeNumerically("==", 52))
				Ω(ps.Internet.DNS.LossNow).Should(BeNumerically(">", 0.32))
				Ω(ps.Internet.DNS.LossNow).Should(BeNumerically("<", 0.34))

				Ω(ps.Gateway.Ping.Count).Should(BeNumerically("==", 52))
				Ω(ps.Gateway.Ping.LossNow).Should(BeNumerically(">", 0.32))
				Ω(ps.Gateway.Ping.LossNow).Should(BeNumerically("<", 0.34))

				Ω(ps.Tunnel.Ping.Count).Should(BeNumerically("==", 52))
				Ω(ps.Tunnel.Ping.LossNow).Should(BeNumerically(">", 0.32))
				Ω(ps.Tunnel.Ping.LossNow).Should(BeNumerically("<", 0.34))
			})
		})
	})
})

var _ = Describe("UpdateLosses()", func() {
	var pingStats *ping.Stats

	Context("streaming average", func() {
		BeforeEach(func() {
			pingStats = &ping.Stats{
				Sent:        42,
				Received:    21,
				Count:       10,
				LossNow:     0.5,
				Loss24Hours: 0.5,
			}

			for i := 0; i < 10; i++ {
				ping.UpdateLosses(pingStats, 1, 0, false)
			}
		})
		It("increases the loss during the last 24 hours", func() {
			Ω(pingStats.Loss24Hours).Should(BeNumerically(">", 0.74))
			Ω(pingStats.Loss24Hours).Should(BeNumerically("<", 0.76))
		})
		It("updates sent/received", func() {
			Ω(pingStats.Sent).Should(BeNumerically("==", 1))
			Ω(pingStats.Received).Should(BeNumerically("==", 0))
		})
	})
	Context("resets count", func() {
		Context("above minimum", func() {
			BeforeEach(func() {
				pingStats = &ping.Stats{
					Count: 4200,
				}

				ping.UpdateLosses(pingStats, 1, 1, true)
			})
			It("integer divides count by two", func() {
				Ω(pingStats.Count).Should(BeNumerically("==", 2100))
			})
		})
		Context("below minimum", func() {
			BeforeEach(func() {
				pingStats = &ping.Stats{
					Count: 42,
				}

				ping.UpdateLosses(pingStats, 1, 1, true)
			})
			It("sets count to 1000", func() {
				Ω(pingStats.Count).Should(BeNumerically("==", 1000))
			})
		})
	})
})
