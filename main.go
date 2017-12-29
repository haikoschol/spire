package main

import (
	"github.com/superscale/spire/config"
	"github.com/superscale/spire/devices"
	"github.com/superscale/spire/devices/deviceInfo"
	"github.com/superscale/spire/devices/exception"
	"github.com/superscale/spire/devices/ota"
	"github.com/superscale/spire/devices/ping"
	"github.com/superscale/spire/devices/sentry"
	"github.com/superscale/spire/devices/stations"
	"github.com/superscale/spire/devices/up"
	"github.com/superscale/spire/mqtt"
	"github.com/superscale/spire/monitoring"
	"log"
)

func main() {
	config.Parse()

	if err := monitoring.InitMonitoring(config.Config.StatsdAddress); err != nil {
		log.Fatal(err)
	}

	broker := mqtt.NewBroker(config.Config.SlashPrefixTopics)
	formations := devices.NewFormationMap()
	loadMessageHandlers(broker, formations)

	devHandler := devices.NewHandler(formations, broker)
	devicesServer := mqtt.NewServer(config.Config.DevicesBind, devHandler.HandleConnection)
	go devicesServer.Run()

	controlServer := mqtt.NewServer(config.Config.ControlBind, broker.HandleConnection)
	controlServer.Run()
}

type registerFn func(*mqtt.Broker, *devices.FormationMap) interface{}

func loadMessageHandlers(broker *mqtt.Broker, formations *devices.FormationMap) {

	regFns := []registerFn{
		deviceInfo.Register,
		exception.Register,
		ota.Register,
		ping.Register,
		up.Register,
		sentry.Register,
		stations.Register,
	}

	for _, register := range regFns {
		register(broker, formations)
	}
}
