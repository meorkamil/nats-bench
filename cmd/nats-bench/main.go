package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/alecthomas/kingpin/v2"
)

const VERSION = "0.0.4"

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
	natsTimeout       = app.Flag("timeout", "NATS context timeout").Default("5").Int()
	natsBatchSize     = app.Flag("batch", "Batch size").Default("100").Int()
)

func main() {
	app.Version(fmt.Sprintf("%s: %s", app.Name, VERSION))
	kingpin.MustParse(app.Parse(os.Args[1:]))

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	slog.Info(fmt.Sprintf("Starting %s mode=%s. nats=%v, subject=%s, stream=%s", VERSION, *appMode, *natsUrl, *natsSubject, *natsStream))

	// Connect to nats
	if err := NewNats(); err != nil {
		slog.Error(fmt.Sprintf("Failed to connect: %v", err))
		os.Exit(1)
	}

	if *appMode == "pub" {
		users := generateUsers(*natsMessageCount, "dot")

		payloads := make([][]byte, 0, len(users))
		for _, u := range users {
			data, _ := json.Marshal(u)
			payloads = append(payloads, data)
		}

		var published int
		var failed int
		var retriesCounter int

		for i := 0; i < len(payloads); i += *natsBatchSize {
			end := min(i+*natsBatchSize, len(payloads))
			batch := payloads[i:end]

			if err := Publish(*natsSubject, batch); err != nil {
				failed += len(batch)

				if reconnErr := NewNats(); reconnErr != nil {
					retriesCounter++
					fmt.Printf("\n")
					slog.Warn(fmt.Sprintf("Force reconnect attempt: %d/%d.", retriesCounter, *natsRetry))

					if retriesCounter >= *natsRetry {
						slog.Error(fmt.Sprintf("Reconnect attempt: %d/%d. %v", retriesCounter, *natsRetry, err))
						os.Exit(1)
					}

					time.Sleep(time.Duration(*natsRetryWait) * time.Second)
				}

				printProgress(published, *natsMessageCount, *natsSubject, failed, retriesCounter)
				continue // skip incrementing published
			}

			published += len(batch)
			printProgress(published, *natsMessageCount, *natsSubject, failed, retriesCounter)

			if *natsPubSubSleep >= 0 {
				time.Sleep(time.Duration(*natsPubSubSleep) * time.Millisecond)
			}
		}

		fmt.Printf("\n")
		slog.Info(fmt.Sprintf("Done. Published %d/%d messages, Failed %d, Retries %d", published, *natsMessageCount, failed, retriesCounter))

	} else if *appMode == "sub" {
		if err := Subscribe(); err != nil {
			slog.Error(fmt.Sprintf("%v", err))
		}

	} else {
		slog.Error(fmt.Sprintf("Unable to start in mode: %s", *appMode))
		os.Exit(1)
	}
}
