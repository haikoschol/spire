package devices_test

import (
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/superscale/spire/devices"
	"github.com/superscale/spire/mqtt"
)

var _ = Describe("Ping Message Handler", func() {

	var broker *mqtt.Broker
	var formations *devices.FormationMap

	var formationID = "00000000-0000-0000-0000-000000000001"
	var deviceName = "1.marsara"
	var topic = fmt.Sprintf("/pylon/%s/wan/ping", deviceName)
	var payload []byte
	var done chan bool

	BeforeEach(func() {
		broker = mqtt.NewBroker()
		formations = devices.NewFormationMap()

		// init the formation map
		upState := map[string]interface{}{"state": "up", "timestamp": time.Now().Unix()}
		formations.PutDeviceState(formationID, deviceName, "up", upState)
		done = make(chan bool)
	})
	JustBeforeEach(func() {
		go func() {
			err := devices.HandlePing(topic, payload, formationID, deviceName, formations, broker)
			Expect(err).NotTo(HaveOccurred())
			done <- true
		}()
	})
	Context("first ping message from this device", func() {
		var firstPingTimestamp time.Time

		BeforeEach(func() {
			pingState := formations.GetDeviceState(formationID, deviceName, "ping")
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
		})
		It("adds ping state to the device state", func() {
			<-done
			_, ok := formations.GetDeviceState(formationID, deviceName, "ping").(*devices.PingState)
			Expect(ok).To(BeTrue())
		})
		It("initializes ping state with the values from the message", func() {
			<-done
			ps, ok := formations.GetDeviceState(formationID, deviceName, "ping").(*devices.PingState)
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
		Context("with subscribers", func() {
			var subscriberConn, brokerConn net.Conn

			BeforeEach(func() {
				subscriberConn, brokerConn = net.Pipe()
				subPkg := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
				subPkg.Topics = []string{"/armada/" + deviceName + "/wan/ping"}
				subPkg.Qoss = []byte{0}
				go broker.Subscribe(subPkg, brokerConn)

				pkg, err := packets.ReadPacket(subscriberConn)
				Expect(err).NotTo(HaveOccurred())
				_, ok := pkg.(*packets.SubackPacket)
				Expect(ok).To(BeTrue())
			})
			It("publishes the ping message", func() {
				pkg, err := packets.ReadPacket(subscriberConn)
				Expect(err).NotTo(HaveOccurred())
				pubPkg, ok := pkg.(*packets.PublishPacket)
				Expect(ok).To(BeTrue())

				Expect(pubPkg.TopicName).To(Equal("/armada/" + deviceName + "/wan/ping"))

				var ps devices.PingState
				err = json.Unmarshal(pubPkg.Payload, &ps)
				Expect(err).NotTo(HaveOccurred())

				var e int64 = 1
				Expect(ps.Version).To(Equal(e))
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
		})
		Context("subsequent ping message from this device", func() {
			JustBeforeEach(func() {
				<-done

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
					err := devices.HandlePing(topic, []byte(fmt.Sprintf(pl, ts.Unix())), formationID, deviceName, formations, broker)
					Expect(err).ToNot(HaveOccurred())
				}
			})
			It("keeps counts and calculates losses", func() {
				ps, ok := formations.GetDeviceState(formationID, deviceName, "ping").(*devices.PingState)
				Expect(ok).To(BeTrue())

				Expect(ps.Timestamp.Unix()).To(Equal(firstPingTimestamp.Unix()))
				Ω(ps.Internet.Ping.Count).Should(BeNumerically("==", 51))
				Ω(ps.Internet.Ping.LossNow).Should(BeNumerically(">", 0.32))
				Ω(ps.Internet.Ping.LossNow).Should(BeNumerically("<", 0.34))

				Ω(ps.Internet.DNS.Count).Should(BeNumerically("==", 51))
				Ω(ps.Internet.DNS.LossNow).Should(BeNumerically(">", 0.32))
				Ω(ps.Internet.DNS.LossNow).Should(BeNumerically("<", 0.34))

				Ω(ps.Gateway.Ping.Count).Should(BeNumerically("==", 51))
				Ω(ps.Gateway.Ping.LossNow).Should(BeNumerically(">", 0.32))
				Ω(ps.Gateway.Ping.LossNow).Should(BeNumerically("<", 0.34))

				Ω(ps.Tunnel.Ping.Count).Should(BeNumerically("==", 51))
				Ω(ps.Tunnel.Ping.LossNow).Should(BeNumerically(">", 0.32))
				Ω(ps.Tunnel.Ping.LossNow).Should(BeNumerically("<", 0.34))
			})
		})
	})
})

var _ = Describe("UpdateLosses()", func() {
	var pingStats *devices.PingStats

	Context("streaming average", func() {
		BeforeEach(func() {
			pingStats = &devices.PingStats{
				Sent:        42,
				Received:    21,
				Count:       10,
				LossNow:     0.5,
				Loss24Hours: 0.5,
			}

			for i := 0; i < 10; i++ {
				devices.UpdateLosses(pingStats, 1, 0, false)
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
				pingStats = &devices.PingStats{
					Count: 4200,
				}

				devices.UpdateLosses(pingStats, 1, 1, true)
			})
			It("integer divides count by two", func() {
				Ω(pingStats.Count).Should(BeNumerically("==", 2100))
			})
		})
		Context("below minimum", func() {
			BeforeEach(func() {
				pingStats = &devices.PingStats{
					Count: 42,
				}

				devices.UpdateLosses(pingStats, 1, 1, true)
			})
			It("sets count to 1000", func() {
				Ω(pingStats.Count).Should(BeNumerically("==", 1000))
			})
		})
	})
})
