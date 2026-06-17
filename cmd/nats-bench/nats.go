package main

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*natsTimeout)*time.Second)
	defer cancel()

	opts := []nats.Option{
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			fmt.Printf("\n")
			slog.Info(fmt.Sprintf("NATS Disconnected: %v. Retrying...", err))
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			fmt.Printf("\n")
			slog.Info(fmt.Sprintf("NATS Reconnected to: %v", nc.ConnectedUrl()))
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			fmt.Printf("\n")
			slog.Info("NATS Connection closed, retries exhausted.")
		}),
		nats.MaxReconnects(*natsRetry),
		nats.ReconnectWait(time.Duration(*natsRetryWait) * time.Second),
	}

	nc, err := nats.Connect(
		*natsUrl,
		opts...,
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
	slog.Info(fmt.Sprintf("Create/Update stream. Name: %s, Subjects: %s", *natsStream, *natsSubject))

	s, err := natsJStr.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:               *natsStream,
		Subjects:           []string{*natsSubject},
		Replicas:           *natsStreamReplica,
		AllowAtomicPublish: true,
	})

	if err != nil {
		return fmt.Errorf(fmt.Sprintf("create stream %v", err))
	}

	natsStr = s

	return nil
}

func Publish(subject string, messages [][]byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*natsTimeout)*time.Second)
	defer cancel()

	batchId := nuid.Next()
	total := len(messages)

	for i, data := range messages {
		msg := &nats.Msg{
			Subject: *natsSubject,
			Data:    data,
			Header:  nats.Header{},
		}

		msg.Header.Set("Nats-Batch-Id", batchId)
		msg.Header.Set("Nats-Batch-Sequence", strconv.Itoa(i+1))

		isLast := i == total-1

		if isLast {
			msg.Header.Set("Nats-Batch-Commit", "1")
			_, err := natsJStr.PublishMsg(ctx, msg)
			if err != nil {
				return fmt.Errorf("batch commit failed: %w", err)
			}
		} else {
			err := natsConn.PublishMsg(msg)
			if err != nil {
				return fmt.Errorf("batch publish failed at %d: %w", i, err)
			}
		}
	}

	return nil
}

func Subscribe() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*natsTimeout)*time.Second)
	defer cancel()

	c, err := natsStr.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   fmt.Sprintf("%s-consumer", *natsStream),
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}

	var received int

	consContext, err := c.Consume(func(msg jetstream.Msg) {
		received++
		printProgress(received, *natsMessageCount, *natsSubject, 0, 0)
		msg.Ack()
	})
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	defer consContext.Stop()

	select {}
}

func printProgress(received, total int, subject string, failed int, retries int) {
	barWidth := 30

	var percent float64
	var filled int
	if total > 0 {
		percent = float64(received) / float64(total) * 100
		filled = int(float64(barWidth) * float64(received) / float64(total))
		if filled > barWidth {
			filled = barWidth
		}
	}

	remaining := barWidth - filled

	if failed > 0 {
		fmt.Printf("\r [%s%s] %.1f%% %d/%d subject=%s failed=%d retries=%d",
			strings.Repeat("=", filled),
			strings.Repeat(" ", remaining),
			percent,
			received,
			total,
			subject,
			failed,
			retries,
		)
	} else {
		fmt.Printf("\r [%s%s] %.1f%% %d/%d subject=%s",
			strings.Repeat("=", filled),
			strings.Repeat(" ", remaining),
			percent,
			received,
			total,
			subject,
		)
	}
}
