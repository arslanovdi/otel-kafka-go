package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	otelkafka "github.com/arslanovdi/otel-kafka-go"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	brokers               = "127.0.0.1:29092,127.0.0.1:29093,127.0.0.1:29094"
	topic                 = "topic"
	group                 = "MyGroup"
	defaultSessionTimeout = 6000
	ReadTimeout           = 1 * time.Second

	instance      = "kafka consumer №1"
	jaegerAddress = "127.0.0.1:4317"
)

func main() {
	// Инициализация глобального провайдера трассировки
	jaeger, err := otelkafka.NewProvider(context.Background(), instance, jaegerAddress)
	if err != nil {
		slog.Error("Failed to create jaeger exporter: ", slog.String("error", err.Error()))
	}

	// Инициализация провайдера трассировки consumer otel-kafka-go
	trace := otelkafka.NewOtelConsumer(instance)

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
		err1 := consumer.Close()
		if err1 != nil {
			slog.Error("Failed to close consumer: ", slog.String("error", err1.Error()))
		}
	}()

	err = consumer.Subscribe(topic, nil)
	if err != nil {
		slog.Error("Failed to subscribe to topic: ", slog.String("error", err.Error()))
		panic("Failed to subscribe to topic")
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

loop:
	for {
		select {
		case <-stop:
			break loop
		default:
			msg, err1 := consumer.ReadMessage(ReadTimeout) // read message with timeout
			if err1 != nil {
				var e kafka.Error
				ok := errors.As(err, &e)
				if ok {
					if e.IsTimeout() { // `err.(kafka.Error).IsTimeout() == true`
						continue // no messages during ReadTimeout
					}
				}

				slog.Error("Consumer error: ", slog.String("error", err1.Error()))
			}

			if otelkafka.Context(msg) == context.Background() {
				slog.Error("Message without root span context")
			}
			trace.OnPoll(msg, group) // tracing poll

			// process message
			func() {
				trace.OnProcess(msg, group) // trace start process message
				fmt.Printf("Message on topic %s: key = %s, value = %s, offset = %d\n",
					*msg.TopicPartition.Topic,
					string(msg.Key),
					string(msg.Value),
					msg.TopicPartition.Offset)
				_, err2 := consumer.CommitMessage(msg)
				if err2 != nil {
					slog.Error("Failed to commit message: ", slog.String("error", err2.Error()))
				}
				trace.OnCommit(msg, group) // trace commit message
			}()

		}
	}

	err = jaeger.Shutdown(context.Background())
	if err != nil {
		slog.Error("Failed to shutdown jaeger exporter: ", slog.String("error", err.Error()))
	}
}
