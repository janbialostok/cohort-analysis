package main

import (
	"encoding/csv"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/ZeFort/chance"
	"github.com/stretchr/testify/assert"
)

var Chance = chance.New()

func makeTemporaryCSVFile() *os.File {
	if tmp, err := ioutil.TempFile("", "testing"); err != nil {
		panic(err)
	} else {
		defer tmp.Close()
		writer := csv.NewWriter(tmp)
		writer.WriteAll([][]string{
			[]string{"id", "name"},
			[]string{"1", "foobar"},
			[]string{"2", "foobar"},
			[]string{"2", "foobar", "extra"},
		})
		return tmp
	}
}

func TestImporter(t *testing.T) {
	validFile := makeTemporaryCSVFile()
	defer os.Remove(validFile.Name())

	_, ok, _ := NewImporter().Open(Chance.String())

	assert.False(t, ok, "should fail on opening non csv file")

	reader, ok, _ := NewImporter().Open(validFile.Name())

	assert.True(t, ok, "should successfully open csv file")
	assert.ElementsMatch(t, []string{"id", "name"}, reader.headers, "should match headers defined in csv")

	row, _ := reader.Read(nil)
	assert.True(t, assert.ObjectsAreEqualValues(
		map[string]interface{}{"id": "1", "name": "foobar"},
		row,
	), "should format row using default transformer")

	row, _ = reader.Read(func(headers, line []string) map[string]interface{} {
		values := make(map[string]interface{})
		for i, value := range line {
			values[headers[i]] = strings.ToUpper(value)
		}
		return values
	})
	assert.True(t, assert.ObjectsAreEqualValues(
		map[string]interface{}{"id": "2", "name": "FOOBAR"},
		row,
	), "should format row by uppercasing string values")

	_, err := reader.Read(nil)
	assert.Error(t, err, "should return an error for mismatched row length")
	assert.Equal(t, "Mismatched values in csv wanted 2 got 3", err.Error(), "should conform to mismatch error message")
}
