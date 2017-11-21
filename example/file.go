package main

import (
	"encoding/json"
	"github.com/trico/sqs"
	"io"
	"log"
	"os"
)

type CsvFormater struct {
	to io.Writer
}

func (c *CsvFormater) Write(p []byte) (n int, err error) {

	log.Print(string(p))
	var s string
	if err := json.Unmarshal(p, &s); err != nil {
		log.Print(err)
		return 0, err
	}

	c.to.Write([]byte(s))

	log.Print(s)

	return len(p), nil
}

func main() {
	producer := sqs.New(sqs.Config{
		QueueName: "test",
	})

	file, _ := os.Create("test.csv")

	w := &CsvFormater{to: file}

	producer.Start()

	producer.Write(w)
}
