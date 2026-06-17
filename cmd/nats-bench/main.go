package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/alecthomas/kingpin/v2"
)

const VERSION = "0.0.3"

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
)

func main() {
	app.Version(fmt.Sprintf("%s: %s", app.Name, VERSION))
	kingpin.MustParse(app.Parse(os.Args[1:]))

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	// Connect to nats
	if err := NewNats(); err != nil {
		slog.Error(fmt.Sprintf("Failed to connect: %v", err))
		os.Exit(1)
	}

	if *appMode == "pub" {
		slog.Info(fmt.Sprintf("Starting mode=%s. nats=%v, subject=%s, stream=%s", *appMode, *natsUrl, *natsSubject, *natsStream))

		users := generateUsers(*natsMessageCount, "dot")

		var message int

		for _, u := range users {

			data, _ := json.Marshal(u)
			if err := Publish(*natsSubject, data); err != nil {
				slog.Error(fmt.Sprintf("Reconnect attempt: %d. nats error: %v", *natsRetry, err))
				os.Exit(1)
			}

			message++
			printProgress(message, *natsMessageCount, *natsSubject)

			if *natsPubSubSleep >= 0 {
				time.Sleep(time.Duration(*natsPubSubSleep) * time.Millisecond)
			}
		}

		fmt.Printf("\n")
		slog.Info(fmt.Sprintf("Done. Published %d/%d messages", message, *natsMessageCount))

	} else if *appMode == "sub" {
		slog.Info(fmt.Sprintf("Starting mode=%s. nats=%v, subject=%s, stream=%s", *appMode, *natsUrl, *natsSubject, *natsStream))

		if err := Subscribe(); err != nil {
			slog.Error(fmt.Sprintf("%v", err))
		}

	} else {
		slog.Error(fmt.Sprintf("Unable to start in mode: %s", *appMode))
		os.Exit(1)
	}
}
