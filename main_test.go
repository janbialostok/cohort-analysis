package main

import (
	"encoding/csv"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func setupTests() (*os.File, *os.File, error, error) {
	db := "./test-cohort-analysis.db"
	ccsv, _ := ioutil.TempFile("", "test-customers")
	ocsv, _ := ioutil.TempFile("", "test-orders")
	resultCSV := "./test-results.csv"
	dbname = &db
	ccsvName := ccsv.Name()
	customerCSV = &ccsvName
	ocsvName := ocsv.Name()
	orderCSV = &ocsvName
	outputPath = &resultCSV

	customerWriter := csv.NewWriter(ccsv)
	orderWriter := csv.NewWriter(ocsv)

	customerWriter.WriteAll([][]string{
		[]string{"id", "created"},
		[]string{"33559", "2015-06-19 23:49:32"},
		[]string{"33563", "2015-06-20 00:09:03"},
	})
	orderWriter.WriteAll([][]string{
		[]string{"id", "order_number", "user_id", "created"},
		[]string{"26444", "1", "33563", "2015-06-25 01:27:40"},
	})
	customerError, orderError := customerWriter.Error(), orderWriter.Error()
	return ccsv, ocsv, customerError, orderError
}

func TestMain(t *testing.T) {
	if !testing.Short() {
		customerFile, orderFile, customerError, orderError := setupTests()
		t.Log("errors", customerError, orderError)
		defer os.Remove(customerFile.Name())
		defer os.Remove(orderFile.Name())
		defer os.Remove("./test-cohort-analysis.db")
		defer os.Remove("./test-results.csv")

		main()
		file, _ := os.Open("./test-results.csv")
		defer file.Close()
		reader := csv.NewReader(file)

		row, _ := reader.Read()
		assert.ElementsMatch(t, []string{"Cohort", "Customers", "0-6"}, row, "should match expected header row")

		row, _ = reader.Read()
		assert.ElementsMatch(t, []string{"06/19/2015-06/25/2015", "2 customers", "50.00% orderers (1)"}, row, "should match first row")
	}
}
