package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type User struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

type RandomCreate struct {
	Count int `json:"count"`
}

type Response struct {
	Message string `json:"message"`
	User    User   `json:"user"`
}

var natsStr jetstream.Stream
var natsJStr jetstream.JetStream
var natsConn *nats.Conn

func NewNats() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	nc, err := nats.Connect(
		*natsUrl,
		nats.MaxReconnects(*natsRetry),
		nats.ReconnectWait(time.Duration(*natsRetryWait)*time.Second),
	)
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("%v", err))
	}

	if nc.ConnectedAddr() == "" {
		return fmt.Errorf("not connected to any server")
	}

	natsConn = nc

	slog.Info(fmt.Sprintf("Connected to NATs Addr: %s, ClusterName: %s, ServerName: %s, ServerVersion: %s",
		nc.ConnectedAddr(),
		nc.ConnectedClusterName(),
		nc.ConnectedServerName(),
		nc.ConnectedServerVersion(),
	))

	// Create jetstream interface
	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("Not connected to any NATS servers")
	}

	natsJStr = js

	// Create stream
	slog.Info(fmt.Sprintf("Create stream. Name: %s, Subjects: %s", *natsStream, *natsSubject))

	s, err := natsJStr.CreateStream(ctx, jetstream.StreamConfig{
		Name:     *natsStream,
		Subjects: []string{*natsSubject},
		Replicas: *natsStreamReplica,
	})

	if err != nil {
		return fmt.Errorf(fmt.Sprintf("create stream %v", err))
	}

	natsStr = s

	return nil
}

func Publish(subject string, data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := natsJStr.Publish(ctx, subject, data)
	if err != nil {
		return fmt.Errorf("%v", err)
	}

	//slog.Debug(fmt.Sprintf("Publish Dup: %v Seq: %v Steam: %v", jStrAck.Duplicate, jStrAck.Sequence, jStrAck.Stream))

	return nil
}

func Subscribe() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := natsStr.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   "CONS",
		AckPolicy: jetstream.AckExplicitPolicy,
	})

	if err != nil {
		return fmt.Errorf("%v", err)
	}

	// Iterate over messages continuously
	go func(c jetstream.Consumer) {

		it, _ := c.Messages()

		for {
			msg, _ := it.Next()
			msg.Ack()
			slog.Info(fmt.Sprintf("Received a JetStream message: %v", string(msg.Data())))
		}

	}(c)

	// Stop and drain NATs
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit
	slog.Info("Shutting down subscriber...")

	slog.Info("Draining NATS...")
	if err := natsConn.Drain(); err != nil {
		slog.Error(fmt.Sprintf("NATs drain error: %s", err))
	}
	return nil
}
