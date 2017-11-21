package sqs

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	s "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"log"
)

const ()

type Config struct {
	QueueName         string
	Client            sqsiface.SQSAPI
	Region            string
	QueueUrl          string
	WaitTimeSeconds   int64
	VisibilityTimeout int64
}

func (c *Config) defaults() {
	c.Client = s.New(session.New(aws.NewConfig()))
	c.QueueUrl = c.queueUrl(c.QueueName)
	c.WaitTimeSeconds = 10
	c.VisibilityTimeout = 10
	c.Region = "eu-west-1"
}

func (c *Config) queueUrl(name string) string {
	fifoQueueName := c.QueueName + ".fifo"

	queueInfo, err := c.Client.GetQueueUrl(&s.GetQueueUrlInput{
		QueueName: &fifoQueueName,
	})

	if err != nil {
		log.Fatal(err)
	}

	return *queueInfo.QueueUrl
}
