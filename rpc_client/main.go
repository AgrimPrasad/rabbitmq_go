package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
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
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, fmt.Sprintf("Failed to declare rpc client queue %s", q.Name))

	corrID := strconv.Itoa(rand.Intn(100))
	input := make(chan string)
	go func() {
		for {
			inputStr := <-input
			err = ch.Publish(
				"",          // exchange
				"rpc_queue", // routing key
				false,       // mandatory
				false,       // immediate
				amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: corrID,
					ReplyTo:       q.Name,
					Body:          []byte(inputStr),
				},
			)
			if err == nil {
				log.Println("Published msg", inputStr, "with correlation ID", corrID)
			}
			failOnError(err, "Failed to publish a message to rpc_queue")
		}
	}()

	go func() {
		log.Println("Enter msg to be published...")
		reader := bufio.NewReader(os.Stdin)
		for {
			log.Print("-> ")
			text, _ := reader.ReadString('\n')
			// convert CRLF to LF
			text = strings.Replace(text, "\n", "", -1)
			input <- text
		}
	}()

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
			if corrID == d.CorrelationId {
				log.Printf("Received a message in rpc client queue %s: %s", q.Name, d.Body)
				dotCount := bytes.Count(d.Body, []byte("."))
				t := time.Duration(dotCount)
				time.Sleep(t * time.Second)
				log.Printf("Done rpc client work")
				d.Ack(false)
			}
		}
	}()

	<-forever
}
