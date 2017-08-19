package main

import (
	"fmt"
	"log"

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
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare queue hello")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register consumer hello")

	q2, err := ch.QueueDeclare(
		"world", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare queue world")

	msgs2, err := ch.Consume(
		q2.Name, // queue
		"",      // consumer
		true,    // auto-ack
		false,   // exclusive
		false,   // no-local
		false,   // no-wait
		nil,     // args
	)
	failOnError(err, "Failed to register consumer world")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message in queue hello: %s", d.Body)
		}
	}()

	go func() {
		for d := range msgs2 {
			log.Printf("Received a message in queue world: %s", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit, press CTRL+C")
	<-forever
}
