package sqs

import (
	s "github.com/aws/aws-sdk-go/service/sqs"
	"io"
	"log"
	"time"
)

type Consumer struct {
	Config
	records chan *UnformatedMessage
}

type UnformatedMessage struct {
	*s.ReceiveMessageOutput
	done bool
}

func New(config Config) *Consumer {
	config.defaults()
	return &Consumer{
		Config:  config,
		records: make(chan *UnformatedMessage),
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
		log.Printf("worker: Start polling from queue %s", c.QueueName)
		output, err := c.Client.ReceiveMessage(options)

		if err != nil {
			panic(err)
		}

		c.records <- &UnformatedMessage{output, false}
	}
}

func (r *UnformatedMessage) Read(p []byte) (n int, err error) {
	if r.done {
		return 0, io.EOF
	}

	for i, b := range []byte(r.GoString()) {
		p[i] = b
	}

	r.done = true

	return len(r.GoString()), nil
}

func (c *Consumer) DeleteMessage(message *UnformatedMessage) {
	_, err := c.Client.DeleteMessage(&s.DeleteMessageInput{
		QueueUrl:      &c.QueueUrl,
		ReceiptHandle: message.Messages[0].ReceiptHandle,
	})

	if err != nil {
		panic(err)
	}
}

func (c *Consumer) Write(b io.Writer) {
	for {
		select {
		case record := <-c.records:

			if len(record.Messages) > 0 {
				c.DeleteMessage(record)
				io.Copy(b, record)
			}
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}
}
