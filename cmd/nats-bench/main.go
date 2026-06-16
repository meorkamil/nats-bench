package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/alecthomas/kingpin/v2"
)

const VERSION = "master"

var (
	app               = kingpin.New("nats-bench", "NATs client publisher").DefaultEnvars()
	appMode           = app.Flag("mode", "Mode pub or sub").Default("pub").String()
	natsUrl           = app.Flag("server", "NATs Endpoint").Short('s').Default("nats://localhost:4222").String()
	natsSubject       = app.Flag("subject", "NATs subject").Default("NATS.BENCH").String()
	natsStream        = app.Flag("stream", "NATs subject").Default("natsbenchstream").String()
	natsStreamReplica = app.Flag("replicas", "Number of replica").Default("1").Int()
	natsMessageCount  = app.Flag("msgs", "Number of message").Default("100").Int()
	natsPubSubSleep   = app.Flag("sleep", "Sleep time between interval in ms").Default("10").Int()
	natsRetry         = app.Flag("retry", "Number of retry to NATS").Default("10").Int()
	natsRetryWait     = app.Flag("retrywait", "Number of retry wait to NATS in second").Default("2").Int()
)

func main() {
	kingpin.MustParse(app.Parse(os.Args[1:]))

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	if *appMode == "pub" {
		slog.Info(fmt.Sprintf("Starting mode=%s. nats=%v, subject=%s, stream=%s", *appMode, *natsUrl, *natsSubject, *natsStream))

		// Connect to nats
		if err := NewNats(); err != nil {
			slog.Error(fmt.Sprintf("Failed to connect: %v", err))
			os.Exit(1)
		}

		users := generateUsers(*natsMessageCount, "dot")

		var message int
		var failed int
		var retriesCounter int

		for _, u := range users {
			data, _ := json.Marshal(u)

			if err := Publish(*natsSubject, data); err != nil {
				failed++

				// Retry logic
				if reconnErr := NewNats(); reconnErr != nil {
					retriesCounter++
					slog.Info(fmt.Sprintf("Reconnect attempt: %d/%d", retriesCounter, *natsRetry))
					if retriesCounter == *natsRetry {
						slog.Error(fmt.Sprintf("Reconnect failed (attempt %d): %v", retriesCounter, reconnErr))
						os.Exit(1)
					}

					// Wait
					time.Sleep(time.Duration(*natsRetryWait) * time.Second)
				}
			}

			message++
			printProgress(message, *natsMessageCount, *natsSubject, failed)

			if *natsPubSubSleep >= 0 {
				time.Sleep(time.Duration(*natsPubSubSleep) * time.Millisecond)
			}
		}

		fmt.Printf("\n")
		slog.Info(fmt.Sprintf("Done. Published %d/%d messages (%d failed)", message-failed, *natsMessageCount, failed))

	} else if *appMode == "sub" {
		slog.Info(fmt.Sprintf("Starting mode=%s. nats=%v, subject=%s, stream=%s", *appMode, *natsUrl, *natsSubject, *natsStream))

	} else {
		slog.Error(fmt.Sprintf("Unable to start in mode: %s", *appMode))
		os.Exit(1)
	}
}

func printProgress(current, total int, subject string, failed int) {
	width := 30
	filled := int(float64(current) / float64(total) * float64(width))

	var bar, arrow, empty string

	if filled == 0 {
		empty = strings.Repeat("-", width)
	} else {
		bar = strings.Repeat("=", filled-1)
		arrow = ">"
		empty = strings.Repeat("-", width-filled)
	}

	pct := float64(current) / float64(total) * 100
	fmt.Printf("\r [%s%s%s] %.1f%% %d/%d failed=%d subject=%s",
		bar, arrow, empty, pct, current, total, failed, subject)
}
