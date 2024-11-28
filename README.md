## OpenTelemetry instrumentation for [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go)

Библиотека реализует трассировку сообщений в/из Kafka с сохранением контекста.

### Install
Manual install:
```bash
go get -u github.com/arslanovdi/otel-kafka-go
```

Golang import:

```go
import "github.com/arslanovdi/otel-kafka-go"
```

### Usage

В приложении должен быть зарегистрирован глобальный провайдер трассировки. 
Например экспортер в jaeger.
```go
const jaeger_address = "127.0.0.1:4317"
jaeger, err := otel.NewProvider(context.Background(), "service_unique_id", jaeger_address)
```

Вся информация о контексте трассировки сохраняется в заголовки kafka сообщения. Соответственно контекст не прерывается от продюсера к консюмеру.

Интерфейс `OtelProvider` обединяет методы Producer и Consumer, использовать если трассировка требуется в обоих направлениях, чтобы не множить сущности.

#### Producer
Инициализация провайдера трассировки
```go
trace := otel.NewOtelProducer("service_unique_id")
```

OtelProducer представлен методом:

```go
OnSend(ctx context.Context, msg *kafka.Message)
```
, где ctx - контекст с родительским trace span, если его нет он создается.
msg - сообщение, которое отправляется в кафку.

Метод `OnSend` вызывать до отправки сообщения в кафку, т.к. он создает новый trace span и интегрирует его в сообщение.

#### Consumer
Инициализация провайдера трассировки
```go
trace := otel.NewOtelConsumer("service_unique_id")
```
Методы OtelConsumer:
```go
OnPoll(msg *kafka.Message, group string)
OnProcess(msg *kafka.Message, group string)
OnCommit(msg *kafka.Message, group string)
Context(message *kafka.Message) context.Context
```

В методы дополнительно передается consumer group, т.к. в сообщении оно не хранится, а по стандарту OpenTelemetry это поле рекомендуется передавать.

Метод `OnPoll` вызывать после получения сообщения через Poll или ReadMessage.

Метод `OnProcess` вызывать перед началом обработки сообщения.

Метод `OnCommit` вызывать после коммита сообщения.

Метод `Context` возвращает контекст с trace span, извлеченный из kafka сообщения. Для трассировки контекста выполнения в следующих сервисах.

### Example

[пример использования библиотеки](https://github.com/arslanovdi/otel-kafka-go/tree/master/example)

## Documentation
[OpenTelemetry API & SDKs for go](https://opentelemetry.io/docs/languages/go/)

[Semantic Conventions for Kafka](https://github.com/open-telemetry/semantic-conventions/blob/main/docs/messaging/kafka.md)