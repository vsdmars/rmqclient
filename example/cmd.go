package main

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"time"

	"github.com/streadway/amqp"
	rmq "github.com/vsdmars/rmqclient"
	"go.uber.org/zap/zapcore"
)

func consume(ctx context.Context, d <-chan amqp.Delivery) error {
	for {
		select {
		case <-ctx.Done():
			return errors.New("application ends")
		case val, ok := <-d:
			if ok {
				fmt.Printf("received msg: %v\n", string(val.Body))
				val.Ack(false)
			} else {
				return errors.New("delivery channel closed")
			}
		}
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	quitSig := func() {
		cancel()
	}

	RegisterHandler(syscall.SIGQUIT, quitSig)
	RegisterHandler(syscall.SIGTERM, quitSig)
	RegisterHandler(syscall.SIGINT, quitSig)

	config := rmq.RmqConfig{
		Username:      "user",
		Password:      "bitnami",
		Host:          "localhost",
		Vhost:         "/vsdmars",
		Port:          5673,
		ReconnectWait: 3 * time.Second,
	}

	// https://godoc.org/go.uber.org/zap/zapcore#Level
	rmq.SetLogLevel(zapcore.DebugLevel)

	r, _ := rmq.NewRmq(ctx, config)
	// Register comsume handler
	r.RegisterHandle("test_queue_1", consume, false, false, true)
	r.Run()

	// Publish message
	go func() {
		// ticker := time.NewTicker(3 * time.Second)
	RESTART:
		var p *rmq.Publish
		for {
			rp, err := r.GetPublish(true)
			if err != nil {
				fmt.Printf("GetPublish err: %v\n", err.Error())
				time.Sleep(3 * time.Second)
				continue
			}

			p = rp
			break
		}

		defer p.Close()

		for {
			select {
			case <-ctx.Done():
				return
				// case <-ticker.C:
			default:
				if err := p.Publish(
					"test_exchange_1",
					"RUN",
					false,
					false,
					amqp.Publishing{
						ContentType:  "text/plain",
						DeliveryMode: 2,
						Body:         []byte("vsdmars testing~"),
						MessageId:    "!42!",
					},
				); err != nil {
					fmt.Printf("Publish error: %v\n", err.Error())
					goto RESTART
				}
			}
		}
	}()

	<-ctx.Done()
	time.Sleep(3 * time.Second)
}
