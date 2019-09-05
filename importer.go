package main

import (
	"encoding/csv"
	"fmt"
	"os"
)

// Importer is used for reading a csv and outputing rows as a map
type Importer struct {
	reader  *csv.Reader
	headers []string
}

// ImportTransformer defines transform functions used to transform csv rows
type ImportTransformer func(headers, line []string) map[string]interface{}

// DefaultTransformer is used when no transform function is specified for Importer.Read
func DefaultTransformer(headers, line []string) map[string]interface{} {
	values := make(map[string]interface{})
	for i, value := range line {
		values[headers[i]] = value
	}
	return values
}

// Open creates a new reader from the specified file path
func (importer Importer) Open(filepath string) (Importer, bool, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return Importer{}, false, err
	}
	reader := csv.NewReader(file)
	if headers, err := reader.Read(); err == nil {
		importer.headers = headers
		importer.reader = reader
	} else {
		return Importer{}, false, err
	}
	return importer, true, nil
}

// MismatchError implements error interface and identifies rows that dont match csv header row length
type MismatchError struct {
	headerLength int
	lineLength   int
}

// Error returns an error message identifying mismatched lengths in csv rows
func (err MismatchError) Error() string {
	return fmt.Sprintf("Mismatched values in csv wanted %d got %d", err.headerLength, err.lineLength)
}

// Read gets the next line of the csv and performs transformation on the data
func (importer Importer) Read(transformer ImportTransformer) (map[string]interface{}, error) {
	if transformer == nil {
		transformer = DefaultTransformer
	}
	line, err := importer.reader.Read()
	if len(line) != len(importer.headers) {
		return nil, MismatchError{len(importer.headers), len(line)}
	}
	if err != nil {
		return nil, err
	}
	return transformer(importer.headers, line), nil
}

// NewImporter creates a new instance of Importer
func NewImporter() Importer {
	return Importer{}
}
