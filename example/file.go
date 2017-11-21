package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"github.com/trico/sqs"
	"log"
	"os"
)

func Printo(msg *sqs.Message) ([]byte, error) {
	b, _ := json.Marshal(*msg.Body)

	var ba bytes.Buffer
	buf := bufio.NewWriter(&ba)

	_ = csv.NewWriter(buf)

	log.Print(b)
	log.Print(ba.Bytes())

	return ba.Bytes(), nil
}

func main() {
	producer := sqs.New(sqs.Config{
		QueueName: "test",
	})

	producer.Start()

	file, _ := os.Create("test.csv")

	producer.Write(file, sqs.HandlerFunc(Printo))
}
