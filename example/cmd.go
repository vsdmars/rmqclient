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

func consume(ctx context.Context, channel *amqp.Channel) error {
	deliveries, err := channel.Consume(
		"test_queue_1",
		"",    // consumer string
		false, // autoack
		false, // exclusive
		false, // nolocal is not supported by rabbitmq
		false, // nowait
		nil,
	)

	if err != nil {
		return errors.New("channel consume creating failed")
	}

	for {
		select {
		case <-ctx.Done():
			return errors.New("application ends")
		case d, ok := <-deliveries:
			if ok {
				fmt.Printf("msg: %v\n", string(d.Body))
				d.Ack(false)
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
		ReconnectWait: time.Duration(3),
	}

	// https://godoc.org/go.uber.org/zap/zapcore#Level
	rmq.SetLogLevel(zapcore.DebugLevel)

	r, _ := rmq.NewRmq(ctx, config)
	r.RegisterConsumeHandle(consume)
	r.Run()

	<-ctx.Done()
	time.Sleep(3 * time.Second)
}
