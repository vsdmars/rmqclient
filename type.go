package rmqclient

import (
	"context"
	"time"

	"github.com/streadway/amqp"
	// _ "gopkg.in/go-playground/validator.v9"
)

type retryError struct {
	error
	reconnect bool
}

type (
	// RmqConfig config for rabbitmq
	RmqConfig struct {
		Username      string        `validate:"required"`
		Password      string        `validate:"required"`
		Host          string        `validate:"required"`
		Vhost         string        `validate:"required"`
		Port          int           `validate:"required"`
		ReconnectWait time.Duration `validate:"isdefault"` // use in seconds
		// Logging       bool   `validate:"isdefault"`
	}

	// RmqStruct is the instance of rabbitmq service
	RmqStruct struct {
		ctx                context.Context
		uuid               string
		config             RmqConfig
		connection         *amqp.Connection
		channel            *amqp.Channel
		consumeHandle      ConsumeHandle    // Consume handler
		connCloseError     chan *amqp.Error // NotifyClose
		channelCancelError chan string      // NotifyCancel
	}

	// ConsumeHandle consumer callback handle's signature
	//
	// Handle should honor passing-in context for cleanup
	ConsumeHandle func(context.Context, *amqp.Channel) error
)
