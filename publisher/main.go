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

	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare queue hello")

	q2, err := ch.QueueDeclare(
		"world", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare queue world")

	input := make(chan string, 2)

	go func() {
		for {
			err = ch.Publish(
				"",     // exchange
				q.Name, // routing key
				false,  // mandatory
				false,  // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte(<-input),
				},
			)
			failOnError(err, "Failed to publish a message to queue hello")

			err = ch.Publish(
				"",      // exchange
				q2.Name, // routing key
				false,   // mandatory
				false,   // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte(<-input),
				},
			)
			failOnError(err, "Failed to publish a message to queue world")
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
