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

	exchName := "logs"

	err = ch.QueueBind(
		q.Name,
		"",
		exchName,
		false,
		nil,
	)
	failOnError(err, fmt.Sprintf("Failed to bind queue %s to exchange %s", q.Name, exchName))

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

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message in queue %s: %s", q.Name, d.Body)
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
