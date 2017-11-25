package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/trico/sqs"
	"log"
	"os"
)

type CsvWriter struct {
	file    *csv.Writer
	records chan []string
}

func (c *CsvWriter) Printo(msg *sqs.Message) error {
	var element map[string]interface{}
	var line []string

	err := json.Unmarshal([]byte(*msg.Body), &element)

	if err != nil {
		log.Fatalln("error:", err)
	}

	line = append(line, element["socas"].(string))
	line = append(line, "cosas")

	fmt.Println(line)

	c.records <- line

	return nil
}

func (c *CsvWriter) populate() {
	fmt.Print(4)

	for {
		select {
		case record := <-c.records:
			c.file.Write(record)
		}
	}
}

func main() {
	file, err := os.Create("test.csv")

	defer file.Close()

	if err != nil {
		log.Fatalln("error", err)
	}

	r := csv.NewWriter(file)

	csv := &CsvWriter{
		r,
		make(chan []string),
	}

	log.Println(1)

	reader := sqs.New(sqs.Config{
		QueueName: "test",
		Handler:   sqs.HandlerFunc(csv.Printo),
	})

	go reader.Start()

	for {
		csv.populate()
	}
}
