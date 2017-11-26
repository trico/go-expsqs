package main

import (
	"encoding/csv"
	"encoding/json"
	"github.com/trico/sqs"
	"log"
	"os"
)

type CsvWriter struct {
	file    *csv.Writer
	records [][]string
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

	c.records = append(c.records, line)

	return nil
}

func (c *CsvWriter) Write() {
	c.file.WriteAll(c.records)
	c.file.Flush()
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
		[][]string{},
	}

	reader := sqs.New(sqs.Config{
		QueueName:        "test",
		Handler:          sqs.HandlerFunc(csv.Printo),
		MessageLimit:     2,
		MessageTimeLimit: 10,
	})

	reader.Start()

	csv.Write()
}
