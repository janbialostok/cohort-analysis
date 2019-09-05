package main

import (
	"encoding/csv"
	"io"
)

type Exporter struct {
	writer  *csv.Writer
	headers []string
}

func (exporter Exporter) Open(w io.Writer) (Exporter, bool, error) {
	writer := csv.NewWriter(w)
	exporter.writer = writer
	return exporter, true, nil
}

func (exporter Exporter) Write(row []string) error {
	if err := exporter.writer.Write(row); err != nil {
		return err
	}
	exporter.writer.Flush()
	return exporter.writer.Error()
}

func NewExporter() Exporter {
	return Exporter{}
}
