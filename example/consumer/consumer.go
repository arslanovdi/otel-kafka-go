package main

import (
	"context"
	"fmt"
	otel "github.com/arslanovdi/otel-kafka-go"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	brokers               = "127.0.0.1:29092,127.0.0.1:29093,127.0.0.1:29094"
	topic                 = "topic"
	group                 = "MyGroup"
	defaultSessionTimeout = 6000
	ReadTimeout           = 1 * time.Second

	instance       = "kafka consumer №1"
	jaeger_address = "127.0.0.1:4317"
)

func main() {

	jaeger, err := otel.NewProvider(context.Background(), instance, jaeger_address)
	if err != nil {
		slog.Error("Failed to create jaeger exporter: ", slog.String("error", err.Error()))
	}

	trace := otel.NewOtelConsumer(instance)

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":       brokers,
		"group.id":                group,
		"session.timeout.ms":      defaultSessionTimeout,
		"enable.auto.commit":      "false",
		"auto.commit.interval.ms": 1000,
		"auto.offset.reset":       "earliest", // earliest - сообщения с commit offset; latest - новые сообщение

	})
	if err != nil {
		slog.Error("Failed to create consumer: ", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer func() {
		err := consumer.Close()
		if err != nil {
			slog.Error("Failed to close consumer: ", slog.String("error", err.Error()))
		}
	}()

	err = consumer.Subscribe(topic, nil)
	if err != nil {
		slog.Error("Failed to subscribe to topic: ", slog.String("error", err.Error()))
		os.Exit(1)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

loop:
	for {
		select {
		case <-stop:
			break loop
		default:
			msg, err := consumer.ReadMessage(ReadTimeout) // read message with timeout
			if err == nil {
				trace.OnPoll(msg, group)

				func() {
					trace.OnProcess(msg, group)
					fmt.Printf("Message on topic %s: key = %s, value = %s\n", *msg.TopicPartition.Topic, string(msg.Key), string(msg.Value))
					_, err := consumer.CommitMessage(msg)
					if err != nil {
						slog.Error("Failed to commit message: ", slog.String("error", err.Error()))
					}
					trace.OnCommit(msg, group)
				}()

			} else if !err.(kafka.Error).IsTimeout() { // TODO process timeout
				slog.Error("Consumer error: ", slog.String("error", err.Error()))
			}
		}
	}

	err = jaeger.Shutdown(context.Background())
	if err != nil {
		slog.Error("Failed to shutdown jaeger exporter: ", slog.String("error", err.Error()))
	}
}
