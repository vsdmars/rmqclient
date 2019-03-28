package rmqclient

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
	// _ "gopkg.in/go-playground/validator.v9"
)

type retryError struct {
	error
	reconnect bool
}

type (
	// RmqConfig is the config type for rmqclient
	RmqConfig struct {
		Username      string        `validate:"required"`
		Password      string        `validate:"required"`
		Host          string        `validate:"required"`
		Vhost         string        `validate:"required"`
		Port          int           `validate:"required"`
		ReconnectWait time.Duration `validate:"isdefault"`
		// Logging       bool   `validate:"isdefault"`
	}

	// RmqStruct is the instance of rabbitmq service
	RmqStruct struct {
		rwlock         sync.RWMutex
		ctx            context.Context // root context
		cctx           atomic.Value    // context.Context, connection context
		uuid           string
		config         RmqConfig
		consumeHandles map[string]*handle // Consume handler, DO NOT USE sync.Map
		connection     atomic.Value       // *amqp.Connection
		connCloseError chan *amqp.Error   // NotifyClose
	}

	// Publish is the publish interface for client
	// https://godoc.org/github.com/streadway/amqp#Channel.Publish
	// https://godoc.org/github.com/streadway/amqp#Channel.NotifyReturn
	// https://godoc.org/github.com/streadway/amqp#Channel.NotifyPublish
	// https://godoc.org/github.com/streadway/amqp#Channel.NotifyConfirm
	Publish struct {
		c          *amqp.Channel
		rmq        *RmqStruct
		closed     bool
		confirm    bool
		pubConfirm chan amqp.Confirmation
	}

	// ConsumeHandle consumer callback handle's signature
	//
	// Handle should honor passing-in context for cleanup
	ConsumeHandle func(context.Context, <-chan amqp.Delivery) error
)

type (
	handle struct {
		h         ConsumeHandle
		cancel    context.CancelFunc
		running   atomic.Value
		autoAck   bool // autoack
		exclusive bool // exclusive
		noWait    bool // nowait
	}
)
