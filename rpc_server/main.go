package main

import (
	"bytes"
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to Rabbitmq server")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.Qos(
		1,     //prefetchCount
		0,     //prefetchSize
		false, //global
	)
	failOnError(err, fmt.Sprintf("Failed to set QoS on channel"))

	q, err := ch.QueueDeclare(
		"rpc_queue", // name
		false,       // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	failOnError(err, fmt.Sprintf("Failed to declare rpc server queue %s", q.Name))

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
			log.Printf("Received a message in rpc server queue %s: %s", q.Name, d.Body)
			dotCount := bytes.Count(d.Body, []byte("."))
			t := time.Duration(dotCount)
			time.Sleep(t * time.Second)

			err = ch.Publish(
				"",        // exchange
				d.ReplyTo, // routing key
				false,     // mandatory
				false,     // immediate
				amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: d.CorrelationId,
					ReplyTo:       q.Name,
					Body:          []byte(fmt.Sprintf("The rpc input had %v dots.", dotCount)),
				},
			)
			failOnError(err, fmt.Sprintf("Failed to publish a message to queue %s", q.Name))

			log.Printf("Done rpc server work")
			d.Ack(false)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit, press CTRL+C")
	<-forever
}
