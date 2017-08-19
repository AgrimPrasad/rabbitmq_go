package main

import (
	"bytes"
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

func main() {
	Consume()
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func Consume() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to Rabbitmq server")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, fmt.Sprintf("Failed to declare queue %s", q.Name))

	q2, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, fmt.Sprintf("Failed to declare queue %s", q2.Name))

	exchName := "logs_topic"
	ch1RoutingKey := "*.critical"
	ch2RoutingKey := "kernel.*"

	err = ch.QueueBind(
		q.Name,
		ch1RoutingKey, // routing key
		exchName,
		false,
		nil,
	)
	failOnError(err, fmt.Sprintf("Failed to bind queue %s with routing key %s", q.Name, ch1RoutingKey))

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, fmt.Sprintf("Failed to register consumer %s", q.Name))

	err = ch.QueueBind(
		q2.Name,
		ch2RoutingKey, // routing key
		exchName,
		false,
		nil,
	)
	failOnError(err, fmt.Sprintf("Failed to bind queue %s with routing key %s", q2.Name, ch2RoutingKey))

	msgs2, err := ch.Consume(
		q2.Name, // queue
		"",      // consumer
		false,   // auto-ack
		false,   // exclusive
		false,   // no-local
		false,   // no-wait
		nil,     // args
	)
	failOnError(err, fmt.Sprintf("Failed to register consumer %s", q2.Name))

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message in queue with routing key %s: %s", ch1RoutingKey, d.Body)
			dotCount := bytes.Count(d.Body, []byte("."))
			t := time.Duration(dotCount)
			time.Sleep(t * time.Second)
			log.Printf("Done work")
			d.Ack(false)
		}
	}()

	go func() {
		for d := range msgs2 {
			log.Printf("Received a message in queue with routing key %s: %s", ch2RoutingKey, d.Body)
			dotCount := bytes.Count(d.Body, []byte("."))
			t := time.Duration(dotCount)
			time.Sleep(t * time.Second)
			log.Printf("Done work")
			d.Ack(false)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit, press CTRL+C")
	<-forever
}
