package mqtt

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"

	bugsnag "github.com/bugsnag/bugsnag-go"
	bugsnagErrors "github.com/bugsnag/bugsnag-go/errors"
	"github.com/superscale/spire/config"
)

// SessionHandler will be run in a goroutine for each connection the server accepts
type SessionHandler func(*Session)

// Server ...
type Server struct {
	bind        string
	listener    net.Listener
	sessHandler SessionHandler
}

// NewServer instantiates a new server that listens on the address passed in "bind"
func NewServer(bind string, sessHandler SessionHandler) *Server {
	if sessHandler == nil {
		return nil
	}

	return &Server{
		bind:        bind,
		sessHandler: sessHandler,
	}
}

// Run ...
func (s *Server) Run() {
	var err error
	if s.listener, err = net.Listen("tcp", s.bind); err != nil {
		log.Println(err)
		notifyBugsnagSync(err, "spire:createListener", bugsnag.MetaData{"Listen": {"Bind": s.bind}})
		os.Exit(1)
	}

	log.Println("listening on", s.bind)
	for {
		if conn, err := s.listener.Accept(); err != nil && err != io.EOF {
			log.Println(err)
		} else {
			go s.handleSession(conn)
		}
	}
}

func (s *Server) handleSession(conn net.Conn) {
	defer func() {
		if err := recover(); err != nil {
			metadata := bugsnag.MetaData{"Client": {
				"IP Address": fmt.Sprintf("%v", conn.RemoteAddr())},
			}

			notifyBugsnag(err, "spire:mqttSession", metadata)
		}
	}()

	s.sessHandler(NewSession(conn, config.Config.IdleConnectionTimeout))
}

func notifyBugsnag(err interface{}, ctx string, metadata bugsnag.MetaData) {
	doNotifyBugsnag(err, ctx, metadata, false)
}

func notifyBugsnagSync(err interface{}, ctx string, metadata bugsnag.MetaData) {
	doNotifyBugsnag(err, ctx, metadata, true)
}

func doNotifyBugsnag(err interface{}, ctx string, metadata bugsnag.MetaData, sync bool) {

	if len(config.Config.BugsnagKey) > 0 {
		notifier := bugsnag.New(bugsnag.Configuration{
			APIKey:       config.Config.BugsnagKey,
			ReleaseStage: config.Config.Environment,
			Synchronous:  sync,
		})

		var stErr error
		var ok bool
		if stErr, ok = err.(*bugsnagErrors.Error); !ok {
			stErr = bugsnagErrors.New(err, 2)
		}

		notifier.Notify(stErr, bugsnag.SeverityError, bugsnag.Context{ctx}, metadata)
	}
}
