package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	otelkafka "github.com/arslanovdi/otel-kafka-go"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	brokers = "127.0.0.1:29092,127.0.0.1:29093,127.0.0.1:29094"

	instance      = "kafka producer №1"
	jaegerAddress = "127.0.0.1:4317"

	ticker = 5 * time.Second
)

var (
	topic = "topic"
	key   = "key"
	value = "value"
)

func main() {
	wg := sync.WaitGroup{}
	stopChain := make(chan struct{})

	// Инициализация глобального провайдера трассировки
	jaeger, err := otelkafka.NewProvider(context.Background(), instance, jaegerAddress)
	if err != nil {
		slog.Error("Failed to create jaeger exporter: ", slog.String("error", err.Error()))
	}

	// Инициализация провайдера трассировки producer otel-kafka-go
	trace := otelkafka.NewOtelProducer(instance)

	cfg := kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"acks":              "all",
	}

	// Инициализация продюсера
	producer, err := kafka.NewProducer(&cfg)
	if err != nil {
		slog.Error("Failed to create producer: ", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer producer.Close()

	// канал для получения отчета о доставке в kafka
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(ticker)
		for {
			select {
			case <-ticker.C: // отправка сообщения по тикеру
				msg := &kafka.Message{
					TopicPartition: kafka.TopicPartition{
						Topic:     &topic,
						Partition: kafka.PartitionAny,
					},
					Key:   []byte(key),
					Value: []byte(value),
				}

				trace.OnSend(context.Background(), msg) // трассировка отправки сообщения

				err = producer.Produce(msg, deliveryChan)
				if err != nil {
					slog.Error("Failed to produce message: ", slog.String("error", err.Error()))
					// retry, skip it in the example
				}
			case <-stopChain:
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case event := <-deliveryChan: // отчеты о доставке / ошибки
				switch ev := event.(type) {
				case *kafka.Message:
					slog.Info("Message produced to topic",
						slog.String("topic", *ev.TopicPartition.Topic),
						slog.Int64("offset", int64(ev.TopicPartition.Offset)))
				case kafka.Error:
					slog.Error("Failed to produce message: ", slog.String("error", ev.Error()))
				}
			case <-stopChain:
				return
			}
		}
	}()

	<-stop

	close(stopChain)

	wg.Wait()

	err = jaeger.Shutdown(context.Background())
	if err != nil {
		slog.Error("Failed to shutdown jaeger exporter: ", slog.String("error", err.Error()))
	}
}
