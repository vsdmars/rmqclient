package rmqclient

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

var svcEndError = errors.New("service ends, cleanup connection loop")

func (rmq *RmqStruct) start() <-chan string {
	status := make(chan string)

	go func() {
		var reconnect = true
		sctx, cancel := context.WithCancel(rmq.ctx)

		defer func() {
			// cleanup consumer goroutine
			cancel()

			if reconnect {
				logger.Info(
					"re-establish connection",
					zap.String("service", serviceName),
					zap.String("uuid", rmq.uuid),
					zap.String(
						"wait_time",
						(rmq.config.ReconnectWait*time.Second).String(),
					),
				)

				time.Sleep(rmq.config.ReconnectWait * time.Second)
			}

			// cleanup status
			close(status)
		}()

		if err := rmq.createConnect(); err != nil {
			return
		}
		status <- "rabbitmq connection established"

		go rmq.consume(sctx)
		status <- "rabbitmq consumer established"

		err := rmq.catchEvent().(retryError)
		reconnect = err.reconnect
		status <- fmt.Sprintf("amqp event occurred: %s", err.Error())
	}()

	return status
}

func (rmq *RmqStruct) catchEvent() error {
	for {
		select {
		case <-rmq.ctx.Done():
			return retryError{
				svcEndError,
				false, // no reconnect
			}
		case err := <-rmq.connCloseError:
			logger.Warn(
				"connection close event occurred",
				zap.String("service", serviceName),
				zap.String("uuid", rmq.uuid),
				zap.String("error", err.Error()),
			)

			return retryError{
				err,
				true, // reconnect
			}
		}
	}
}

// createConnect creates amqp connection
func (rmq *RmqStruct) createConnect() error {
	amqpURL := amqp.URI{
		Scheme:   "amqp",
		Host:     rmq.config.Host,
		Username: rmq.config.Username,
		Password: "XXXXX",
		Port:     rmq.config.Port,
		Vhost:    rmq.config.Vhost,
	}

	logger.Info(
		"connect URL",
		zap.String("service", serviceName),
		zap.String("uuid", rmq.uuid),
		zap.String("url", amqpURL.String()),
	)

	amqpURL.Password = rmq.config.Password

	// tcp connection timeout in 3 seconds
	myconn, err := amqp.DialConfig(
		amqpURL.String(),
		amqp.Config{
			Vhost: rmq.config.Vhost,
			Dial: func(network, addr string) (net.Conn, error) {
				return net.DialTimeout(network, addr, 3*time.Second)
			},
			Heartbeat: 10 * time.Second,
			Locale:    "en_US"},
	)
	if err != nil {
		logger.Warn(
			"open connection failed",
			zap.String("service", serviceName),
			zap.String("uuid", rmq.uuid),
			zap.String("error", err.Error()),
		)

		return err
	}

	rmq.connection = myconn
	rmq.connCloseError = make(chan *amqp.Error)
	// https://godoc.org/github.com/streadway/amqp#Channel.NotifyClose
	// amqp library is responsible for closing the error channel
	// Connection exceptions will be broadcast to all open channels and
	// all channels will be closed, where channel exceptions will only
	// be broadcast to listeners to this channel.
	rmq.connection.NotifyClose(rmq.connCloseError)
	return nil
}

// createChannel creates amqp channel
func (rmq *RmqStruct) createChannel() (*amqp.Channel, error) {
	myChannel, err := rmq.connection.Channel()
	if err != nil {
		logger.Warn(
			"create channel failed",
			zap.String("service", serviceName),
			zap.String("uuid", rmq.uuid),
			zap.String("error", err.Error()),
		)

		return nil, err
	}

	channelCancelError := make(chan string)
	// https://godoc.org/github.com/streadway/amqp#Channel.NotifyCancel
	// If a queue doesn't exist, the channel is marked as close state,
	// which makes amqp library hard to use.
	// Thus, we compensate this by create new channel for each queue.
	myChannel.NotifyCancel(channelCancelError)

	go func() {
		for {
			select {
			case <-rmq.ctx.Done():
				return
			case tag, ok := <-channelCancelError:
				// If a queue doesn't exist (deleted or never declared)
				// in this channel, channelCancelError is closed immediately.
				if !ok {
					logger.Warn(
						"channel cancel event closed",
						zap.String("service", serviceName),
						zap.String("uuid", rmq.uuid),
					)
				} else {
					logger.Warn(
						"channel cancel event occurred",
						zap.String("service", serviceName),
						zap.String("uuid", rmq.uuid),
						zap.String("queue", tag),
					)
				}

				_ = myChannel.Close()
				return
			}
		}
	}()

	return myChannel, nil
}

func (rmq *RmqStruct) consume(ctx context.Context) {
	// check handler that is not running in every 10 seconds.
	ticker := time.NewTicker(10 * time.Second)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			func() {
				defer rmq.rwlock.RUnlock()
				rmq.rwlock.RLock()

				for name, handle := range rmq.consumeHandles {
					r := handle.running.Load().(bool)
					if r {
						continue
					}

					channel, err := rmq.createChannel()
					if err != nil {
						continue
					}

					d, err := channel.Consume(
						name,
						name,             // consumerTag
						handle.autoAck,   // autoack
						handle.exclusive, // exclusive
						false,            // nolocal is not supported by rabbitmq
						handle.noWait,    // nowait
						nil,
					)
					if err != nil {
						logger.Error(
							"channel.Consume error",
							zap.String("service", serviceName),
							zap.String("uuid", rmq.uuid),
							zap.String("queue", name),
						)

						continue
					}

					hctx, hcancel := context.WithCancel(ctx)
					handle.cancel = hcancel
					handle.running.Store(true)

					// avoid race condition
					chandle := handle

					go func() {
						defer func() {
							chandle.running.Store(false)
							hcancel()
						}()

						if err := chandle.h(hctx, d); err != nil {
							logger.Warn(
								"consume handler ended",
								zap.String("service", serviceName),
								zap.String("uuid", rmq.uuid),
								zap.String("queue", name),
								zap.String("error", err.Error()),
							)
						}
					}()
				}
			}()
		}
	}
}

// TODO: fill the guts~
func (rmq *RmqStruct) validate() error {
	return nil
}
