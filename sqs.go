package sqs

import (
	s "github.com/aws/aws-sdk-go/service/sqs"
	"io"
	"log"
	"time"
)

type Consumer struct {
	Config
	records chan *s.ReceiveMessageOutput
}

type Message struct {
	*s.Message
}

type HandlerFunc func(msg *Message) ([]byte, error)

func (f HandlerFunc) HandleMessage(msg *Message) ([]byte, error) {
	return f(msg)
}

type Handler interface {
	HandleMessage(msg *Message) ([]byte, error)
}

func New(config Config) *Consumer {
	config.defaults()
	return &Consumer{
		Config:  config,
		records: make(chan *s.ReceiveMessageOutput),
	}
}

func (c *Consumer) Start() {
	go c.Get()
}

func (c *Consumer) Get() {
	options := &s.ReceiveMessageInput{
		QueueUrl:          &c.QueueUrl,
		WaitTimeSeconds:   &c.WaitTimeSeconds,
		VisibilityTimeout: &c.VisibilityTimeout,
	}

	for {
		log.Printf("Start %d seconds circle from queue %s", c.WaitTimeSeconds, c.QueueName)
		output, err := c.Client.ReceiveMessage(options)

		if err != nil {
			panic(err)
		}

		c.records <- output
	}
}

func (c *Consumer) DeleteMessage(message *s.Message) {
	_, err := c.Client.DeleteMessage(&s.DeleteMessageInput{
		QueueUrl:      &c.QueueUrl,
		ReceiptHandle: message.ReceiptHandle,
	})

	if err != nil {
		panic(err)
	}
}

func (c *Consumer) Write(b io.Writer, h Handler) {
	for {
		select {
		case record := <-c.records:

			if len(record.Messages) > 0 {
				for _, message := range record.Messages {
					c.DeleteMessage(message)

					r, _ := h.HandleMessage(&Message{message})

					b.Write(r)
				}
			}
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}
}
