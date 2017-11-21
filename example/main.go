package main

import (
	"github.com/trico/sqs"
	"os"
)

func main() {
	producer := sqs.New(sqs.Config{
		QueueName: "test",
	})

	producer.Start()

	producer.Write(os.Stdout)
}
