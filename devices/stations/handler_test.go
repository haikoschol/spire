package stations_test

import (
	"encoding/json"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/superscale/spire/devices"
	"github.com/superscale/spire/devices/stations"
	"github.com/superscale/spire/mqtt"
	"github.com/superscale/spire/testutils"
)

var _ = Describe("Stations Handler", func() {

	var broker *mqtt.Broker
	var formations *devices.FormationMap
	var recorder *testutils.PubSubRecorder
	var payload interface{}
	var topic string
	var publishedStationsMsg *stations.Message

	var deviceName = "1.marsara"
	var formationID = "00000000-0000-0000-0000-000000000001"

	BeforeEach(func() {
		payload = nil
		broker = mqtt.NewBroker(false)
		formations = devices.NewFormationMap()
		recorder = testutils.NewPubSubRecorder()

		broker.Subscribe("matriarch/1.marsara/stations", recorder)
		formations.AddDevice(deviceName, formationID)
		stations.Register(broker, formations)
	})
	JustBeforeEach(func() {
		if payload != nil {
			broker.Publish(topic, payload)
		}

		if _, m := recorder.First(); m != nil {
			var ok bool
			publishedStationsMsg, ok = m.(*stations.Message)
			Expect(ok).To(BeTrue())
		}
	})
	Describe("survey messages", func() {
		var deviceSurveyMsg = []byte(`{
			"dev": {
				"wlp58s0": {
					"survey": "Survey data from wlp58s0\n frequency:      2412 MHz\n noise:        -95 dBm\n channel active time:    70039858 ms\n channel busy time:    36751702 ms\n channel receive time:   33080329 ms \n  channel transmit time:    2157771 ms \nSurvey data from wlp58s0 \n  frequency:      5200 MHz \n  noise:        -95 dBm \n  channel active time:    59 ms \n  channel busy time:    44 ms \n  channel receive time:   27 ms \n  channel transmit time:    3 ms"
				},
				"wlan-private-a": {
					"survey": "Survey data from wlan-private-a \n        frequency:                      5180 MHz [in use] \n        noise:                          -106 dBm \n        channel active time:            635043 ms \n        channel busy time:              137081 ms \n        channel receive time:           89174 ms \n        channel transmit time:          864 ms \nSurvey data from wlan-private-a \n        frequency:                      5200 MHz \n        noise:                          -108 dBm \n        channel active time:            146 ms \n        channel busy time:              16 ms"
				}
			}
		}`)

		var surveyRecorder *testutils.PubSubRecorder
		var publishedSurveyMsg map[string]*stations.WifiSurvey

		BeforeEach(func() {
			topic = "pylon/1.marsara/wifi/poll"
			payload = deviceSurveyMsg
			surveyRecorder = testutils.NewPubSubRecorder()
			broker.Subscribe("matriarch/1.marsara/wifi/survey", surveyRecorder)
		})
		JustBeforeEach(func() {
			_, m := surveyRecorder.First()
			var ok bool
			publishedSurveyMsg, ok = m.(map[string]*stations.WifiSurvey)
			Expect(ok).To(BeTrue())
		})
		It("publishes a survey with three entries", func() {
			Expect(len(publishedSurveyMsg)).To(Equal(3))
		})
		It("picks the entry with the longest 'channel active time'", func() {
			s, exists := publishedSurveyMsg["5200 MHz"]
			Expect(exists).To(BeTrue())
			Expect(s.ChannelActiveTime).To(Equal("146 ms"))
		})
	})
	Describe("wifi stations messages", func() {
		var stationsMsg = []byte(`{
			"dev": {
				"wlan0-private-a": {
					"stations": "Station 4C:7C:5F:FF:FF:FF (on wlan0-private-a)\n inactive time:  370 ms\n rx bytes:       55456\n rx packets:     814\n tx bytes:       36043\n tx packets:     260\n tx retries:     0\n tx failed:      2\n signal:         -45 dBm\n signal avg:     -46 dBm\n tx bitrate:     6.0 MBit/s\n rx bitrate:     24.0 MBit/s\n authorized:     yes\n authenticated:  yes\n preamble:       long\n WMM/WME:        yes\n MFP:            no\n TDLS peer:      no\n connected time: 162 seconds"
				},
				"wlan1-private-a": {
					"stations": "Station 4C:7C:5F:FE:FE:FE (on wlan1-public-g)\n inactive time:  23 seconds\n rx bytes:       4711\n rx packets:     814\n tx bytes:       36043\n tx packets:     260\n tx retries:     0\n tx failed:      2\n signal:         -45 dBm\n signal avg:     -46 dBm\n tx bitrate:     6.0 MBit/s\n rx bitrate:     24.0 MBit/s\n authorized:     yes\n authenticated:  yes\n preamble:       long\n WMM/WME:        yes\n MFP:            no\n TDLS peer:      no\n connected time: 162 seconds"
				}
			}
		}`)

		BeforeEach(func() {
			topic = "pylon/1.marsara/wifi/poll"
			payload = stationsMsg
		})
		It("stores the parsed stations info in the formation state", func() {
			formations.RLock()
			defer formations.RUnlock()
			stationsState, ok := formations.GetState(formationID, stations.Key).(*stations.State)
			Expect(ok).To(BeTrue())

			wifiStations := stationsState.WifiStations
			Expect(len(wifiStations)).To(Equal(2))
			Expect(wifiStations["4C:7C:5F:FF:FF:FF"]["tx packets"]).To(Equal("260"))
			Expect(wifiStations["4C:7C:5F:FE:FE:FE"]["tx packets"]).To(Equal("260"))
		})
		It("includes the parsed stations in the published stations message", func() {
			Expect(len(publishedStationsMsg.Private)).To(Equal(2))

			s1 := publishedStationsMsg.Private[0]
			Expect(s1["mac"]).To(SatisfyAny(Equal("4C:7C:5F:FE:FE:FE"), Equal("4C:7C:5F:FF:FF:FF")))
			Expect(s1["tx packets"]).To(Equal("260"))

			s2 := publishedStationsMsg.Private[1]
			Expect(s2["mac"]).To(SatisfyAny(Equal("4C:7C:5F:FE:FE:FE"), Equal("4C:7C:5F:FF:FF:FF")))
			Expect(s2["tx packets"]).To(Equal("260"))
		})
		It("includes 'inactive_time' in the published stations message", func() {
			s1 := publishedStationsMsg.Private[0]
			Expect(s1["inactive_time"]).ToNot(BeNil())
		})
	})
	Describe("assoc event messages", func() {
		var assocMsg = []byte(`{
			"station": "4C:7C:5F:FF:FF:FF",
			"action": "assoc"
		}`)

		BeforeEach(func() {
			topic = "pylon/1.marsara/wifi/event"
			payload = assocMsg

			formations.RLock()
			defer formations.RUnlock()
			state := formations.GetState(formationID, stations.Key)
			Expect(state).To(BeNil())
		})
		It("adds the new station", func() {
			formations.RLock()
			defer formations.RUnlock()
			stationsState, ok := formations.GetState(formationID, stations.Key).(*stations.State)

			Expect(ok).To(BeTrue())
			station, exists := stationsState.WifiStations["4C:7C:5F:FF:FF:FF"]
			Expect(exists).To(BeTrue())
			Expect(station["mac"]).To(Equal("4C:7C:5F:FF:FF:FF"))
		})
	})
	Describe("disassoc event messages", func() {
		var assocMsg = []byte(`{
			"station": "4C:7C:5F:FF:FF:FF",
			"action": "disassoc"
		}`)

		BeforeEach(func() {
			topic = "pylon/1.marsara/wifi/event"
			payload = assocMsg

			state := &stations.State{
				WifiStations: map[string]stations.WifiStation{
					"4C:7C:5F:FF:FF:FF": {"mac": "4C:7C:5F:FF:FF:FF", "mode": "private"},
					"4C:7C:5F:FE:FE:FE": {"mac": "4C:7C:5F:FE:FE:FE", "mode": "public"},
				},
			}

			formations.Lock()
			defer formations.Unlock()
			formations.PutState(formationID, stations.Key, state)
		})
		It("removes the station", func() {
			formations.RLock()
			defer formations.RUnlock()
			stationsState, ok := formations.GetState(formationID, stations.Key).(*stations.State)

			Expect(ok).To(BeTrue())
			Expect(len(stationsState.WifiStations)).To(Equal(1))
			_, exists := stationsState.WifiStations["4C:7C:5F:FF:FF:FF"]
			Expect(exists).To(BeFalse())
		})
	})
	Describe("thing discovery messages", func() {
		var discoveryMsg = []byte(`{
			"address": "1.2.3.4",
			"thing": {
				"i am": "a toaster"
			}
		}`)

		BeforeEach(func() {
			topic = "pylon/1.marsara/things/discovery"
			payload = discoveryMsg

			formations.RLock()
			defer formations.RUnlock()
			Expect(formations.GetState(formationID, stations.Key)).To(BeNil())
		})
		It("adds the thing", func() {
			formations.RLock()
			defer formations.RUnlock()
			stationsState, ok := formations.GetState(formationID, stations.Key).(*stations.State)
			Expect(ok).To(BeTrue())

			Expect(len(stationsState.Things)).To(Equal(1))

			thing, exists := stationsState.Things["1.2.3.4"]
			Expect(exists).To(BeTrue())
			Expect(thing.IP).To(Equal("1.2.3.4"))
			Expect(thing.Thing["i am"]).To(Equal("a toaster"))
		})
		Describe("subsequent 'net' messages", func() {
			JustBeforeEach(func() {
				broker.Publish("pylon/1.marsara/net", []byte(`{
					"mac": [
						{"ip": "1.2.3.4", "mac": "12:12:12:12:12:12"}
					]
				}`))

				_, m := recorder.Last()
				Expect(m).NotTo(BeNil())
				publishedStationsMsg = m.(*stations.Message)
			})
			It("add the mac address of the thing", func() {
				formations.RLock()
				defer formations.RUnlock()
				stationsState, ok := formations.GetState(formationID, stations.Key).(*stations.State)
				Expect(ok).To(BeTrue())

				thing := stationsState.Things["1.2.3.4"]
				Expect(thing.MAC).To(Equal("12:12:12:12:12:12"))
			})
			It("includes the thing in the published stations message", func() {
				Expect(len(publishedStationsMsg.Thing)).To(Equal(1))

				thing := publishedStationsMsg.Thing[0]
				Expect(thing.MAC).To(Equal("12:12:12:12:12:12"))
				Expect(thing.IP).To(Equal("1.2.3.4"))
			})
		})
	})
	Describe("sys/facts messages", func() {

		var sysMsg = []byte(`{
			"board": {
				"switch": {
				   "switch0": {
					   "enable": true,
					   "reset": true,
					   "ports": [
						   {
							   "num": 0,
							   "device": "eth1",
							   "need_tag": false
						   },
						   {
							   "num": 2,
							   "role": "lan"
						   },
						   {
							   "num": 3,
							   "role": "lan"
						   },
						   {
							   "num": 4,
							   "role": "lan"
						   },
						   {
							   "num": 5,
							   "role": "lan"
						   },
						   {
							   "num": 6,
							   "device": "eth0",
							   "need_tag": false
						   },
						   {
							   "num": 1,
							   "role": "wan"
						   }
					   ]
				   }
				}
			}
		}`)

		BeforeEach(func() {
			topic = "pylon/1.marsara/sys/facts"
			payload = sysMsg
			Expect(formations.GetDeviceState(deviceName, "cpu_ports")).To(BeNil())
		})
		It("stores the cpu ports", func() {
			cpuPorts, ok := formations.GetDeviceState(deviceName, "cpu_ports").([]string)
			Expect(ok).To(BeTrue())

			Expect(len(cpuPorts)).To(Equal(2))
			Expect(cpuPorts[0]).To(Equal("0"))
			Expect(cpuPorts[1]).To(Equal("6"))
		})
	})
	Describe("net messages", func() {
		var state *stations.State

		BeforeEach(func() {
			state = stations.NewState()

			state.WifiStations["aa:aa:aa:aa:aa:aa"] = stations.WifiStation{
				"ip":   "1.2.3.4",
				"mode": "private",
			}

			state.Things["2.3.4.5"] = &stations.Thing{
				MAC:           "bb:bb:bb:bb:bb:bb",
				IP:            "2.3.4.5",
				LastUpdatedAt: time.Now().UTC(),
			}

			state.Things["4.5.6.7"] = &stations.Thing{
				MAC:           "ff:ff:ff:ff:ff:ff",
				IP:            "4.5.6.7",
				LastUpdatedAt: time.Now().UTC(),
			}

			formations.Lock()
			defer formations.Unlock()
			formations.PutState(formationID, stations.Key, state)

			topic = "pylon/1.marsara/net"
			payload = []byte(`{
				"mac": [
					{"mac": "aa:aa:aa:aa:aa:aa", "ip": "1.2.3.4"},
					{"mac": "bb:bb:bb:bb:bb:bb", "ip": "2.3.4.5"},
					{"mac": "cc:cc:cc:cc:cc:cc", "ip": "3.4.5.6"}
				],
				"bridge": {
					"macs": {
						"public": "port no\tmac addr\t\tis local?\tageing timer\n  4\tbb:bb:bb:bb:bb:bb\tno\t\t   1.5\n  4\tcc:cc:cc:cc:cc:cc\tyes\t\t   1.51\n",
						"private": "port no\tmac addr\t\tis local?\tageing timer\n  4\taa:aa:aa:aa:aa:aa\tno\t\t   1.02\n"
					}
				}
			}`)
		})
		It("infers lan stations from arp information", func() {
			formations.RLock()
			defer formations.RUnlock()
			state := formations.GetState(formationID, stations.Key).(*stations.State)

			Expect(len(state.LanStations)).To(Equal(1))
			lanStation, exists := state.LanStations["cc:cc:cc:cc:cc:cc"]
			Expect(exists).To(BeTrue())
			Expect(lanStation.IP).To(Equal("3.4.5.6"))
		})
		It("sets 'age' and 'local' from bridge info", func() {
			formations.RLock()
			defer formations.RUnlock()
			state := formations.GetState(formationID, stations.Key).(*stations.State)

			s1 := state.WifiStations["aa:aa:aa:aa:aa:aa"]
			Expect(s1["age"]).To(BeNumerically(">", 0))
			Expect(s1["age"]).To(BeNumerically("<", 2))
			Expect(s1["local"]).To(BeFalse())

			s2 := state.Things["2.3.4.5"]
			Expect(s2.Age).To(BeNumerically(">", 0))
			Expect(s2.Age).To(BeNumerically("<", 2))
			Expect(s2.Local).To(BeFalse())

			s3 := state.LanStations["cc:cc:cc:cc:cc:cc"]
			Expect(s3.Age).To(BeNumerically(">", 0))
			Expect(s3.Age).To(BeNumerically("<", 2))
			Expect(s3.Local).To(BeTrue())
		})
		It("includes the lan station in the published stations message", func() {
			Expect(len(publishedStationsMsg.Other)).To(Equal(1))

			lanStation := publishedStationsMsg.Other[0]
			Expect(lanStation.MAC).To(Equal("cc:cc:cc:cc:cc:cc"))
			Expect(lanStation.IP).To(Equal("3.4.5.6"))
		})
		It("includes bridge info in the published stations message", func() {
			s1 := publishedStationsMsg.Private[0]
			Expect(s1["age"]).To(BeNumerically(">", 0))
			Expect(s1["age"]).To(BeNumerically("<", 2))
			Expect(s1["local"]).To(BeFalse())

			s2 := publishedStationsMsg.Thing[0]
			Expect(s2.Age).To(BeNumerically(">=", float64(0)))
			Expect(s2.Age).To(BeNumerically("<=", float64(2)))
			Expect(s2.Local).To(BeFalse())

			s3 := publishedStationsMsg.Other[0]
			Expect(s3.Age).To(BeNumerically(">=", float64(0)))
			Expect(s3.Age).To(BeNumerically("<=", float64(2)))
			Expect(s3.Local).To(BeTrue())
		})
		It("includes 'seen' in the published stations message", func() {
			s1 := publishedStationsMsg.Private[0]
			Expect(s1["seen"]).ToNot(BeNil())

			s2 := publishedStationsMsg.Thing[0]
			Expect(s2.Seen).To(BeNumerically(">", 0))

			s3 := publishedStationsMsg.Other[0]
			Expect(s3.Seen).To(BeNumerically(">", 0))
		})
		It("includes 'inactive_time' in the published stations message", func() {
			s2 := publishedStationsMsg.Thing[0]
			Expect(s2.InactiveTime).To(BeNumerically(">", 0))

			s3 := publishedStationsMsg.Other[0]
			Expect(s3.InactiveTime).To(BeNumerically(">", 0))
		})
		Context("things timeout", func() {
			BeforeEach(func() {
				state.Things["4.5.6.7"].LastUpdatedAt = time.Now().UTC().Add(-10 * time.Minute)
			})
			It("removes timed out things before publishing", func() {
				formations.RLock()
				defer formations.RUnlock()
				state := formations.GetState(formationID, stations.Key).(*stations.State)
				Expect(len(state.Things)).To(Equal(1))

				_, exists := state.Things["4.5.6.7"]
				Expect(exists).To(BeFalse())
			})
			It("does not include the timed out thing in the published stations message", func() {
				Expect(len(publishedStationsMsg.Thing)).To(Equal(1))
				Expect(publishedStationsMsg.Thing[0].IP).NotTo(Equal("4.5.6.7"))
			})
		})
		Context("lan stations timeout", func() {
			BeforeEach(func() {
				state.LanStations["ee:ee:ee:ee:ee:ee"] = &stations.LanStation{
					LastUpdatedAt: time.Now().UTC().Add(-20 * time.Minute),
				}
			})
			It("removes timed out lan stations before publishing", func() {
				formations.RLock()
				defer formations.RUnlock()
				state := formations.GetState(formationID, stations.Key).(*stations.State)
				_, exists := state.LanStations["ee:ee:ee:ee:ee:ee"]
				Expect(exists).To(BeFalse())
			})
			It("does not include the timed out lan stations in the published stations message", func() {
				Expect(len(publishedStationsMsg.Other)).To(Equal(1))
			})
		})
	})
	Describe("odhcpd messages", func() {

		var dhcpRecorder *testutils.PubSubRecorder

		BeforeEach(func() {
			dhcpRecorder = testutils.NewPubSubRecorder()
			broker.Subscribe("matriarch/1.marsara/dhcp/leases", dhcpRecorder)

			topic = "pylon/1.marsara/odhcpd"
			payload = []byte("wlan0\n11:11:11:11:11:11\t192.168.1.100\t4711\tclient1\n22:22:22:22:22:22\t192.168.1.101\t1337\tclient2\nwlan1\n33:33:33:33:33:33\t192.168.1.102\t2342\tclient3\n")
		})
		It("publishes the dhcp information under dhcp/leases", func() {
			_, msg := dhcpRecorder.First()
			Expect(msg).NotTo(BeNil())

			dhcpState, ok := msg.(stations.DHCPState)
			Expect(ok).To(BeTrue())

			wlan0 := dhcpState.Get("wlan0")
			Expect(len(wlan0)).To(Equal(2))

			c1 := wlan0[0]
			Expect(c1.MAC).To(Equal("11:11:11:11:11:11"))
			Expect(c1.IP).To(Equal("192.168.1.100"))
			Expect(c1.Hostname).To(Equal("client1"))
			Expect(c1.TTL).To(Equal("4711"))

			c2 := wlan0[1]
			Expect(c2.MAC).To(Equal("22:22:22:22:22:22"))
			Expect(c2.IP).To(Equal("192.168.1.101"))
			Expect(c2.Hostname).To(Equal("client2"))
			Expect(c2.TTL).To(Equal("1337"))

			wlan1 := dhcpState.Get("wlan1")
			Expect(len(wlan1)).To(Equal(1))

			c3 := wlan1[0]
			Expect(c3.MAC).To(Equal("33:33:33:33:33:33"))
			Expect(c3.IP).To(Equal("192.168.1.102"))
			Expect(c3.Hostname).To(Equal("client3"))
			Expect(c3.TTL).To(Equal("2342"))
		})
	})
	Describe("sends current stations state on subscribe", func() {
		var brokerSession, subscriberSession *mqtt.Session
		var message *stations.Message

		BeforeEach(func() {
			state := stations.NewState()

			state.WifiStations["aa:aa:aa:aa:aa:aa"] = stations.WifiStation{
				"ip":   "1.2.3.4",
				"mode": "private",
			}

			formations.Lock()
			formations.PutDeviceState(formationID, deviceName, stations.Key, state)
			formations.Unlock()

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
			message = new(stations.Message)
			err = json.Unmarshal(pubPkg.Payload, message)
			Expect(err).NotTo(HaveOccurred())
		})
		AfterEach(func() {
			brokerSession.Close()
			subscriberSession.Close()
		})
		It("publishes a stations message for the device", func() {
			Expect(len(message.Private)).To(Equal(1))
			Expect(message.Private[0]["ip"]).To(Equal("1.2.3.4"))
		})
	})
})
