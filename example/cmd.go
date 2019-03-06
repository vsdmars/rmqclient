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

func consume_1(ctx context.Context, d <-chan amqp.Delivery) error {
	for {
		fmt.Println("FUCK in consume_1")
		select {
		case <-ctx.Done():
			return errors.New("application ends")
		case val, ok := <-d:
			if ok {
				fmt.Printf("msg: %v\n", string(val.Body))
				val.Ack(false)
			} else {
				return errors.New("delivery channel closed")
			}
		}
	}
}

func consume_2(ctx context.Context, d <-chan amqp.Delivery) error {
	for {
		select {
		case <-ctx.Done():
			return errors.New("application ends")
		case val, ok := <-d:
			if ok {
				fmt.Printf("msg: %v\n", string(val.Body))
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
		ReconnectWait: time.Duration(3),
	}

	// https://godoc.org/go.uber.org/zap/zapcore#Level
	rmq.SetLogLevel(zapcore.DebugLevel)

	r, _ := rmq.NewRmq(ctx, config)
	r.RegisterHandle("test_queue_1", consume_1, false, false, true)
	r.Run()

	r.RegisterHandle("test_queue_2", consume_2, false, false, true)
	// r.RegisterHandle("test_queue_3", consume_2, false, false, true)
	// r.UnregisterHandle("test_queue_3")

	<-ctx.Done()
	time.Sleep(3 * time.Second)
}
