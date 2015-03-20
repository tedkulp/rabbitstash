package main

import (
	"fmt"
	"flag"
	"log"
	"time"
	"net"
	"encoding/json"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func createJsonPacket(d amqp.Delivery) string {
	var msg map[string]interface{}
	json.Unmarshal([]byte(d.Body), &msg)

	f := map[string]interface{} {
		"@timestamp": d.Timestamp,
		"@version": "1",
		"severity": "INFO",
		"correlation_id": d.CorrelationId,
		"message_id": d.MessageId,
		"reply_to": d.ReplyTo,
		"routing_key": d.RoutingKey,
	}

	if (len(msg) > 0) {
		f["@message"] = msg
	}

	if (len(d.Headers) > 0) {
		f["headers"] = d.Headers
	}

	if d.Timestamp.IsZero() {
		f["@timestamp"] = time.Now()
	}

	if f["correlation_id"] == nil || f["correlation_id"] == "" {
		delete(f, "correlation_id")
	}

	if f["message_id"] == nil || f["message_id"] == "" {
		delete(f, "message_id")
	}

	if f["reply_to"] == nil || f["reply_to"] == "" {
		delete(f, "reply_to")
	}

	str, err := json.Marshal(f)
	if err != nil {
		fmt.Println("Error encoding JSON")
		return "{}"
	}

	return string(str)
}

func main() {
	amqpHost := flag.String("amqphost", "192.168.3.21", "Hostname of the AMQP server")
	amqpPort := flag.Int("amqpport", 5672, "Port of the AMQP server")
	amqpUser := flag.String("amqpuser", "guest", "User on the AMQP server")
	amqpPass := flag.String("amqppass", "guest", "Password on the AMQP server")
	amqpQueue := flag.String("amqpqueue", "logstash", "Queue on the AMQP server")

	logstashHost := flag.String("loghost", "192.168.1.33", "Hostname of the Logstash server")
	logstashPort := flag.Int("logport", 9997, "Port of the Logstash server")

	flag.Parse()

	sourceAddr := fmt.Sprintf("amqp://%s:%s@%s:%d/", *amqpUser, *amqpPass, *amqpHost, *amqpPort)
	fmt.Println(sourceAddr)
	conn, err := amqp.Dial(sourceAddr)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	destAddr := fmt.Sprintf("%s:%d", *logstashHost, *logstashPort)
	fmt.Println(destAddr)
	outConn, err := net.Dial("udp", destAddr)
	failOnError(err, "Failed to connect to Logstash")
	defer outConn.Close()

	q, err := ch.QueueDeclare(
		*amqpQueue, // name
		true,       // durable
		false,      // delete when usused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name,     // queue
		"",         // consumer
		true,       // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for msg := range msgs {
			packet := createJsonPacket(msg)
			fmt.Println(packet)
			buf := []byte(packet)
			_, err := outConn.Write(buf)
			if err != nil {
				fmt.Println(err)
			}
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
