package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type OrderProducer struct {
	producer   *kafka.Producer
	topic      string
	deliverych chan kafka.Event
}

func NewOrderProducer(p *kafka.Producer, topic string) *OrderProducer {
	return &OrderProducer{
		producer:   p,
		topic:      topic,
		deliverych: make(chan kafka.Event, 10000),
	}
}

func (op *OrderProducer) placeOrder(orderType string, size int) error {
	var (
		format  = fmt.Sprintf("%s - %v", orderType, size)
		payload = []byte(format)
	)

	err := op.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &op.topic,
			Partition: kafka.PartitionAny,
		},
		Value: payload,
	},
		op.deliverych,
	)
	if err != nil {
		log.Fatal(err)
	}
	<-op.deliverych
	fmt.Printf("Ordem produzida: %v - %v\n", op.topic, size)
	return nil
}

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "foo",
		"acks":              "all",
	})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
	}

	go func() {

	}()

	op := NewOrderProducer(p, "HVSE")
	for i := 0; i < 1000; i++ {
		if err := op.placeOrder("HVSE", i+1); err != nil {
			log.Fatal(err)
		}
		time.Sleep(time.Second * 1)
	}
}
