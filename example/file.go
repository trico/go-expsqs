package main

import (
	"encoding/json"
	"github.com/trico/sqs"
	"io"
	"log"
	"os"
)

func Printo(msg *sqs.Message) ([]byte, error) {
	var element map[string]interface{}
	var line string

	err := json.Unmarshal([]byte(*msg.Body), &element)

	if err != nil {
		log.Fatalln("error:", err)
	}

	line += element["a"].(string)
	line += ";"
	line += "1"
	line += "\n"

	return []byte(line), nil
}

func main() {
	producer := sqs.New(sqs.Config{
		QueueName: "test",
	})

	producer.Start()

	file, _ := os.Create("test.csv")

	multi := io.MultiWriter(file, os.Stdout)

	producer.Write(multi, sqs.HandlerFunc(Printo))
}
