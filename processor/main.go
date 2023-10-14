package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	topic := "HVSE"
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "foo",
		"auto.offset.reset": "smallest",
	})
	if err != nil {
		log.Fatal(err)
	}

	err = consumer.Subscribe(topic, nil)
	if err != nil {
		log.Fatal(err)
	}
	for {
		ev := consumer.Poll(100)
		switch e := ev.(type) {
		case *kafka.Message:
			fmt.Printf("Processando ordem da fila %s\n", string(e.Value))
		case *kafka.Error:
			fmt.Printf("Erro ao consumir ordem da fila %v\n", e)
		}
	}
}
