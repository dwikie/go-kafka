package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type OrderPlacer struct {
	producer *kafka.Producer
	topic    string
}

func NewOrderPlacer(p *kafka.Producer, topic string) *OrderPlacer {
	return &OrderPlacer{
		producer: p,
		topic:    topic,
	}
}

func (op *OrderPlacer) placeOrder(orderType string, size rune) error {
	err := op.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &op.topic, Partition: kafka.PartitionAny},
		Value:          []byte("Foo"),
	},
		deliveryCh,
	)

	if err != nil {
		log.Fatal(err)
	}

	<-deliveryCh
	time.Sleep(time.Second * 3)
}

func main() {
	topic := "HVSE"
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "foo",
		"acks":              "all"})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
	}

	go func() {
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
				fmt.Printf("consumed message from the que: %s\n", string(e.Value))
			case *kafka.Error:
				fmt.Printf("%v\n", e)
			}
		}
	}()

	deliveryCh := make(chan kafka.Event, 10000)
	for {
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte("Foo"),
		},
			deliveryCh,
		)

		if err != nil {
			log.Fatal(err)
		}

		<-deliveryCh
		time.Sleep(time.Second * 3)
	}
}
