package sqs

import (
	s "github.com/aws/aws-sdk-go/service/sqs"
	"log"
	"sync/atomic"
	"time"
)

type Consumer struct {
	Config
	options     *s.ReceiveMessageInput
	messages    chan *s.ReceiveMessageOutput
	done        chan struct{}
	numMessages uint64
	stop        bool
}

type Message struct {
	*s.Message
}

func New(config Config) *Consumer {
	config.defaults()
	consumer := &Consumer{
		Config: config,
		options: &s.ReceiveMessageInput{
			QueueUrl:          &config.QueueUrl,
			WaitTimeSeconds:   &config.WaitTimeSeconds,
			VisibilityTimeout: &config.VisibilityTimeout,
		},
		done:     make(chan struct{}),
		messages: make(chan *s.ReceiveMessageOutput),
	}

	go consumer.poll()

	return consumer
}

func (c *Consumer) Start() {
	defer close(c.done)

	for {
		select {
		case output := <-c.messages:
			if len(output.Messages) > 0 {
				for _, message := range output.Messages {
					go c.handleMessage(message)
				}
			}
		case <-c.done:
			return
		}
	}
}

func (c *Consumer) poll() {
	log.Print("Polling from queue")
	tick := time.NewTicker(time.Second * c.Config.MessageTimeLimit)

	for {
		select {
		case <-tick.C:
			log.Print("Timeout reached")
			c.Stop()
			return
		default:
			if c.stop == true {
				return
			}

			output, err := c.Client.ReceiveMessage(c.options)

			if err != nil {
				log.Print(err)
			}

			c.messages <- output
		}
	}
}

func (c *Consumer) handleMessage(m *s.Message) {
	message := &Message{m}
	atomic.AddUint64(&c.numMessages, 1)
	c.DeleteMessage(m)

	if c.MessageLimit != 0 && int64(atomic.LoadUint64(&c.numMessages)) == c.MessageLimit {
		log.Print("Limit reached")
		c.Stop()
	}

	c.Config.Handler(message)
}

func (c *Consumer) DeleteMessage(message *s.Message) {
	_, err := c.Client.DeleteMessage(&s.DeleteMessageInput{
		QueueUrl:      &c.QueueUrl,
		ReceiptHandle: message.ReceiptHandle,
	})

	if err != nil {
		log.Print(err)
	}
}

func (c *Consumer) Stop() {
	log.Print("Stopping")

	c.done <- struct{}{}
	c.stop = true

	log.Print("Stopped")
}
