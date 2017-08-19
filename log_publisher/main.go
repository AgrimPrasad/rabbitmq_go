package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/streadway/amqp"
)

func main() {
	Publish()
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func Publish() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to Rabbitmq server")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	exchName := "logs_topic"
	err = ch.ExchangeDeclare(
		exchName, // name
		"topic",  // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, fmt.Sprintf("Failed to declare exchange %s", exchName))

	input := make(chan string, 3)

	ch1RoutingKey := "app.critical"
	ch2RoutingKey := "app.info"
	ch3RoutingKey := "kernel.info"

	go func() {
		for {
			err = ch.Publish(
				exchName,      // exchange
				ch1RoutingKey, // routing key
				false,         // mandatory
				false,         // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte(<-input),
				},
			)
			failOnError(err, fmt.Sprintf("Failed to publish a log with routing key %s", ch1RoutingKey))

			err = ch.Publish(
				exchName,      // exchange
				ch2RoutingKey, // routing key
				false,         // mandatory
				false,         // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte(<-input),
				},
			)
			failOnError(err, fmt.Sprintf("Failed to publish a log with routing key %s", ch2RoutingKey))

			err = ch.Publish(
				exchName,      // exchange
				ch3RoutingKey, // routing key
				false,         // mandatory
				false,         // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte(<-input),
				},
			)
			failOnError(err, fmt.Sprintf("Failed to publish a log with routing key %s", ch3RoutingKey))

		}
	}()

	log.Println("Enter msg to be published...")
	reader := bufio.NewReader(os.Stdin)
	for {
		log.Print("-> ")
		text, _ := reader.ReadString('\n')
		// convert CRLF to LF
		text = strings.Replace(text, "\n", "", -1)
		input <- text
	}
}
