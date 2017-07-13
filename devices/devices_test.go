package devices_test

import (
	"net"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/superscale/spire/devices"
	"gopkg.in/redis.v5"
)

var _ = Describe("Device Message Handlers", func() {

	var devs *devices.DeviceMap
	var devMsgHandler *devices.MessageHandler
	var server net.Conn
	var client net.Conn
	var response packets.ControlPacket

	BeforeEach(func() {
		devs = devices.NewDeviceMap()
		devMsgHandler = devices.NewMessageHandler(devs, redisClient)
		server, client = net.Pipe()

		upState, err := redisClient.HGet(devices.CacheKey("1.marsara"), "up_state").Result()
		Expect(err == redis.Nil || upState == "down").To(BeTrue())

		go func() {
			connPkg := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
			connPkg.ClientIdentifier = "1.marsara"
			Expect(connPkg.Write(client)).NotTo(HaveOccurred())
		}()

		go func() {
			devMsgHandler.HandleConnection(server)
		}()

		response, err = packets.ReadPacket(client)
		Expect(err).NotTo(HaveOccurred())
	})
	Context("connect", func() {
		It("sends CONNACK", func() {
			_, ok := response.(*packets.ConnackPacket)
			Expect(ok).To(BeTrue())
		})
		It("adds the device", func() {
			dev, err := devs.Get("1.marsara")
			Expect(err).NotTo(HaveOccurred())
			Expect(dev).NotTo(BeNil())
		})
		It("sets 'up_state' in redis to 'up'", func() {
			upState, err := redisClient.HGet(devices.CacheKey("1.marsara"), "up_state").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(upState).To(Equal("up"))
		})
	})
	Context("disconnect", func() {
		Context("by sending DISCONNECT", func() {
			BeforeEach(func() {
				pkg := packets.NewControlPacket(packets.Disconnect)
				Expect(pkg.Write(client)).NotTo(HaveOccurred())
			})
			It("removes the device", func() {
				_, err := devs.Get("1.marsara")
				Expect(err).To(Equal(devices.ErrDevNotFound))
			})
			It("sets 'up_state' in redis to 'down'", func() {
				upState, err := redisClient.HGet(devices.CacheKey("1.marsara"), "up_state").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(upState).To(Equal("down"))
			})
		})
		Context("by closing the connection", func() {
			BeforeEach(func() {
				Expect(client.Close()).ToNot(HaveOccurred())
				buf := make([]byte, 5)
				server.Read(buf)
			})
			It("removes the device", func() {
				// Since the function under test runs in a goroutine, we need this to avoid a race condition. :(
				time.Sleep(time.Millisecond * 1)
				_, err := devs.Get("1.marsara")
				Expect(err).To(Equal(devices.ErrDevNotFound))
			})
			It("sets 'up_state' in redis to 'down'", func() {
				time.Sleep(time.Millisecond * 1)
				upState, err := redisClient.HGet(devices.CacheKey("1.marsara"), "up_state").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(upState).To(Equal("down"))
			})
		})
	})
})
