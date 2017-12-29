package config

import (
	"time"

	"github.com/caarlos0/env"
)

// Params defines all possible config params
type Params struct {
	Environment           string        `env:"SPIRE_ENV"  envDefault:"prod"`
	DevicesBind           string        `env:"SPIRE_DEVICES_BIND"  envDefault:":1883"`
	ControlBind           string        `env:"SPIRE_CONTROL_BIND"  envDefault:":1884"`
	BugsnagKey            string        `env:"SPIRE_BUGSNAG_KEY"`
	LiberatorBaseURL      string        `env:"SPIRE_LIBERATOR_BASE_URL"  envDefault:"https://api.superscale.io"`
	LiberatorJWTToken     string        `env:"SPIRE_LIBERATOR_JWT_TOKEN,required"`
	IdleConnectionTimeout time.Duration `env:"SPIRE_IDLE_CONNECTION_TIMEOUT"  envDefault:"30s"`
	SentryDynamoDBTable   string        `env:"SPIRE_SENTRY_DYNAMODB_TABLE,required"`
	SlashPrefixTopics     bool          `env:"SPIRE_SLASH_PREFIX_TOPICS"  envDefault:"true"`
	StatsdAddress         string        `env:"SPIRE_STATSD_ADDRESS"`
}

// Config is the global handle for accessing runtime configuration
var Config = &Params{}

// Parse environment variables into Config
func Parse() {
	err := env.Parse(Config)
	if err != nil {
		panic(err)
	}
}
