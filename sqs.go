package sqs

import (
	s "github.com/aws/aws-sdk-go/service/sqs"
	"log"
)

type Consumer struct {
	Config
	options *s.ReceiveMessageInput
}

type Message struct {
	*s.Message
}

func New(config Config) *Consumer {
	config.defaults()
	return &Consumer{
		Config: config,
		options: &s.ReceiveMessageInput{
			QueueUrl:          &config.QueueUrl,
			WaitTimeSeconds:   &config.WaitTimeSeconds,
			VisibilityTimeout: &config.VisibilityTimeout,
		},
	}
}

func (c *Consumer) Start() {
	for {
		log.Printf("Start %d seconds circle from queue %s", c.WaitTimeSeconds, c.QueueName)
		output, err := c.Client.ReceiveMessage(c.options)

		if err != nil {
			panic(err)
		}

		if len(output.Messages) > 0 {
			for _, message := range output.Messages {
				go c.Config.Handler(&Message{message})
				c.DeleteMessage(message)
			}
		}
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
