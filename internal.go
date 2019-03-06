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

// defaultHandle is a template for handler writers
func defaultHandle(ctx context.Context, d <-chan amqp.Delivery) error {
	logger.Info(
		"default rabbitmq consume handler",
		zap.String("service", serviceName),
	)

	<-ctx.Done()
	return errors.New("service ends")
}

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

		// create rabbitmq connection
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
			// case tag, ok := <-rmq.channelCancelError:
			// // interestingly, the amqp library won't trigger
			// // this event iff we are not using amqp.Channel
			// // to declare the queue.
			// // However, if the queue is autodeleted, this event will be triggered.
			// // Log only.

			// if !ok {
			// fmt.Println("SHIT, closed!")

			// logger.Warn(
			// "channel cancel event closed",
			// zap.String("service", serviceName),
			// zap.String("uuid", rmq.uuid),
			// )

			// rmq.channelCancelError = make(chan string)
			// rmq.channel.NotifyCancel(rmq.channelCancelError)

			// continue
			// }

			// logger.Warn(
			// "channel cancel event occurred",
			// zap.String("service", serviceName),
			// zap.String("uuid", rmq.uuid),
			// zap.String("queue", tag),
			// )
			// }
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
	// amqp library is resposible for closing the error channel
	// https://godoc.org/github.com/streadway/amqp#Channel.NotifyClose
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

		return err
	}

	// These can be sent from the server when a queue is deleted or
	// when consuming from a mirrored queue where the master has just failed
	// (and was moved to another node)
	channelCancelError = make(chan string)
	// https://godoc.org/github.com/streadway/amqp#Channel.NotifyCancel
	// If the queue doesn't exist, the channel is marked as close state,
	// which makes amqp library VERY hard to use.
	// Thus, we compensate this by create new channel for each queue.
	myChannel.NotifyCancel(channelCancelError)
	return nil
}

func (rmq *RmqStruct) consume(ctx context.Context) {
	go func() {
		// runs every 10 seconds check for new consumer
		ticker := time.NewTicker(10 * time.Second)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				rmq.rwlock.RLock()

				for name, handle := range rmq.consumeHandles {
					if handle.running {
						continue
					}

					d, err := rmq.channel.Consume(
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
					handle.running = true

					go func() {
						defer func() {
							handle.running = false
							hcancel()
						}()

						if err := handle.h(hctx, d); err != nil {
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
				rmq.rwlock.RUnlock()
			}
		}
	}()
}

// TODO: fill the guts~
func (rmq *RmqStruct) validate() error {
	return nil
}
