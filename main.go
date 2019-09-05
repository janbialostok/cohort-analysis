package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	dbname         = flag.String("db", "./cohort-analysis.db", "specify the db file in which SQL data should be stored")
	customerCSV    = flag.String("customers", "./data/customers.csv", "specify the path to the customer data")
	orderCSV       = flag.String("orders", "./data/orders.csv", "specify the path to the order data")
	datetimeLayout = flag.String("datetimeLayout", "2006-01-02 15:04:05 UTC", "specify the layout of datetime")
	timezone       = flag.String("timezone", "UTC", "specify the timezone the UTC defined datetimes should be stored in")
	runImport      = flag.String("import", "true", "specify if import of data should run")
	outputPath     = flag.String("output", "./results.csv", "specify the file path for the results output")
	stdoutMode     = flag.Bool("stdout", false, "specify that the output csv should be written to stdout")
)

var customerSchema = map[string]string{
	"id":      "int not null primary key",
	"created": "datetime not null",
}

var orderSchema = map[string]string{
	"id":           "int not null primary key",
	"order_number": "int not null",
	"user_id":      "int not null",
	"created":      "datetime not null",
}

func makeTables() (SQL, error) {
	shouldImport := true
	if *runImport == "false" {
		shouldImport = false
	}
	db, err := ConnectDB(shouldImport, *dbname)
	if err != nil {
		return db, fmt.Errorf("Failed to connect to database with error %s", err.Error())
	}

	if err := CreateTable(db, "customers", customerSchema); err != nil {
		return db, fmt.Errorf("Failed to create customers table with error %s", err.Error())
	}

	if err := CreateTable(db, "orders", orderSchema); err != nil {
		return db, fmt.Errorf("Failed to create orders table with error %s", err.Error())
	}

	return db, nil
}

func makeCustomerImportTransformer(timezone *time.Location) func([]string, []string) map[string]interface{} {
	return func(_, line []string) map[string]interface{} {
		customer := make(map[string]interface{})
		customer["id"], _ = strconv.Atoi(line[0])

		created := line[1]
		if datetime, err := time.Parse(*datetimeLayout, fmt.Sprintf("%s UTC", created)); err != nil {
			log.Println("failed to parse date", err)
			customer["created"] = ""
		} else {
			customer["created"] = datetime.In(timezone).Format("2006-01-02T15:04:05")
		}
		return customer
	}
}

func makeOrderImportTransformer(timezone *time.Location) func([]string, []string) map[string]interface{} {
	return func(_, line []string) map[string]interface{} {
		order := make(map[string]interface{})
		order["id"], _ = strconv.Atoi(line[0])
		order["order_number"], _ = strconv.Atoi(line[1])
		order["user_id"], _ = strconv.Atoi(line[2])

		created := line[3]
		if datetime, err := time.Parse(*datetimeLayout, fmt.Sprintf("%s UTC", created)); err != nil {
			log.Println("failed to parse date", err)
			order["created"] = ""
		} else {
			order["created"] = datetime.In(timezone).Format("2006-01-02T15:04:05")
		}
		return order
	}
}

func importCustomers(db SQL, timezone *time.Location) error {
	customerTransformer := makeCustomerImportTransformer(timezone)
	if importer, ok, err := NewImporter().Open(*customerCSV); !ok {
		return err
	} else {
		hasSkipped := false
		for {
			if value, err := importer.Read(customerTransformer); err != nil {
				if err == io.EOF {
					break
				} else if _, ok := err.(MismatchError); ok {
					if !hasSkipped {
						hasSkipped = true
						continue
					} else {
						break
					}
				} else {
					return err
				}
			} else {
				hasSkipped = false
				if err := Insert(db, "customers", []interface{}{value["id"], value["created"]}); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func importOrders(db SQL, timezone *time.Location) error {
	orderTransformer := makeOrderImportTransformer(timezone)
	if importer, ok, err := NewImporter().Open(*orderCSV); !ok {
		return err
	} else {
		hasSkipped := false
		for {
			if value, err := importer.Read(orderTransformer); err != nil {
				if err == io.EOF {
					break
				} else if _, ok := err.(MismatchError); ok {
					if !hasSkipped {
						hasSkipped = true
						continue
					} else {
						break
					}
				} else {
					return err
				}
			} else {
				hasSkipped = false
				if err := Insert(db, "orders", []interface{}{value["id"], value["order_number"], value["user_id"], value["created"]}); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func getStartDate(db SQL) (*time.Time, error) {
	var startDate time.Time

	if rows, err := Query(db, "customers", []string{"*"}, QueryOptions{
		Limit:   1,
		OrderBy: "created",
		Asc:     true,
	}); err != nil {
		return nil, err
	} else {
		var id string
		var created string
		if rows.Next() {
			if err := rows.Scan(&id, &created); err != nil {
				return nil, err
			}
		}
		rows.Close()
		startDate, _ = time.Parse("2006-01-02T15:04:05Z", created)
	}
	return &startDate, nil
}

func getEndDate(db SQL) (*time.Time, error) {
	var endDate time.Time

	if rows, err := Query(db, "customers", []string{"*"}, QueryOptions{
		Limit:   1,
		OrderBy: "created",
		Asc:     false,
	}); err != nil {
		return nil, err
	} else {
		var id string
		var created string
		if rows.Next() {
			if err := rows.Scan(&id, &created); err != nil {
				return nil, err
			}
		}
		rows.Close()
		endDate, _ = time.Parse("2006-01-02T15:04:05Z", created)
	}
	return &endDate, nil
}

type Orders struct {
	UniqueOrders    map[string]bool
	FirstTimeOrders int
}

type Cohort struct {
	Dates             string
	MaxDaysFromCreate int
	Customers         map[string]time.Time
	HasOrder          map[string]bool
	Orders            map[int]Orders
}

func aggregateOrders(db SQL, query string, customers map[string]time.Time, hasOrder map[string]bool) (map[int]Orders, int, error) {
	aggregatedOrders := map[int]Orders{}
	// query orders in ascending date order to ensure that first time orders are tied to earliest order date
	orders, err := Query(db, "orders", []string{"user_id", "created"}, QueryOptions{
		OrderBy: "created",
		Asc:     true,
		Where:   query,
	})
	if err != nil {
		return nil, 0, err
	}
	defer orders.Close()
	var (
		userID  string
		created string
	)
	maxDays := 0
	for orders.Next() {
		err := orders.Scan(&userID, &created)
		if err != nil {
			return nil, 0, err
		}
		// check if customer for given order exists in customer creation datetime map
		if customerCreateDate, ok := customers[userID]; ok {
			orderCreateDate, _ := time.Parse("2006-01-02T15:04:05Z", created)
			// get the number of days from customer creation the order was placed
			daysSinceCustomerCreate := int(orderCreateDate.Sub(customerCreateDate).Hours() / float64(24))
			// create an order for given number of days if it does not alrady exist
			if _, ok := aggregatedOrders[daysSinceCustomerCreate]; !ok {
				aggregatedOrders[daysSinceCustomerCreate] = Orders{
					make(map[string]bool),
					0,
				}
			}
			// track max days from customer creation
			if maxDays < daysSinceCustomerCreate {
				maxDays = daysSinceCustomerCreate
			}
			// check if order was already placed by that customer that day
			aggregatedOrders[daysSinceCustomerCreate].UniqueOrders[userID] = true
			if _, ok := hasOrder[userID]; !ok {
				hasOrder[userID] = true
				order := aggregatedOrders[daysSinceCustomerCreate]
				// increment first time orders count if customer had not ever placed an order
				order.FirstTimeOrders++
				aggregatedOrders[daysSinceCustomerCreate] = order
			}
		}
	}
	return aggregatedOrders, maxDays, nil
}

func generateCohort(db SQL, customers *sql.Rows, dates string) (Cohort, error) {
	var id string
	var created string
	cohort := Cohort{
		dates,
		0,
		make(map[string]time.Time),
		make(map[string]bool),
		make(map[int]Orders),
	}
	// create query for orders table based on customer ids
	orderWhereQuery := strings.Builder{}
	orderWhereQuery.WriteString("user_id IN (")
	isFirst := true
	defer customers.Close()
	for customers.Next() {
		err := customers.Scan(&id, &created)
		if err != nil {
			return cohort, err
		}
		createdDate, _ := time.Parse("2006-01-02T15:04:05Z", created)
		cohort.Customers[id] = createdDate
		if isFirst {
			orderWhereQuery.WriteString(id)
		} else {
			orderWhereQuery.WriteString(fmt.Sprintf(", %s", id))
		}
		isFirst = false
	}
	orderWhereQuery.WriteString(")")
	// query for orders that come from specified customers and aggregate on days from sign up date
	aggregatedOrders, maxDays, err := aggregateOrders(db, orderWhereQuery.String(), cohort.Customers, cohort.HasOrder)
	if err != nil {
		return cohort, err
	}
	cohort.Orders = aggregatedOrders
	cohort.MaxDaysFromCreate = maxDays
	return cohort, nil
}

func makeCohortRows(cohort Cohort, headers *OrderedStringSet) [][]string {
	// set default header values
	headers.Add("Cohort").Add("Customers")
	uniqueOrders := []string{cohort.Dates, fmt.Sprintf("%d customers", len(cohort.Customers))}
	firstTimeOrders := []string{"", ""}
	start := 0
	end := 6
	for {
		// iteratively go through 7 day ranges until day exceeds max number of days for order from customer creation
		if start > cohort.MaxDaysFromCreate {
			break
		}
		// set unique day ranges to headers
		headers.Add(fmt.Sprintf("%d-%d", start, end))
		uniqueCount := 0
		firstOrderCount := 0
		// iterate through map of orders by day for each day in range aggregating unique orders and first time orders
		for days := start; days <= end; days++ {
			if orders, ok := cohort.Orders[days]; ok {
				uniqueCount += len(orders.UniqueOrders)
				firstOrderCount += orders.FirstTimeOrders
			}
		}
		// format unique order count for csv row
		if uniqueCount == 0 {
			uniqueOrders = append(uniqueOrders, "0% orderers (0)")
		} else {
			uniqueOrders = append(uniqueOrders, fmt.Sprintf(
				"%.2f%% orderers (%d)",
				(float64(uniqueCount)/float64(len(cohort.Customers)))*100,
				uniqueCount,
			))
		}
		// format first time order count for csv row
		if firstOrderCount == 0 {
			firstTimeOrders = append(firstTimeOrders, "0% 1st time (0)")
		} else {
			firstTimeOrders = append(firstTimeOrders, fmt.Sprintf(
				"%.2f%% 1st time (%d)",
				(float64(firstOrderCount)/float64(len(cohort.Customers)))*100,
				firstOrderCount,
			))
		}
		start += 7
		end += 7
	}
	return [][]string{
		uniqueOrders,
		firstTimeOrders,
	}
}

func writeCohortRows(output io.Writer, cohort [][]string) error {
	if exporter, ok, err := NewExporter().Open(output); ok {
		err := exporter.Write(cohort[0])
		if err != nil {
			return err
		}
		for i := len(cohort) - 1; i > 0; i -= 2 {
			if err := exporter.Write(cohort[i-1]); err != nil {
				return err
			}
			if err := exporter.Write(cohort[i]); err != nil {
				return err
			}
		}
	} else {
		return err
	}
	return nil
}

func main() {
	flag.Parse()
	// create tables necessary for storing customer and order data
	db, err := makeTables()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	// load custom timezone data
	tz, err := time.LoadLocation(*timezone)
	if err != nil {
		log.Println("failed to load timezone", err)
		tz, _ = time.LoadLocation("UTC")
	}
	// import data from csvs and load data to sqlite instance
	if *runImport != "false" {
		log.Println("importing customers")
		if err := importCustomers(db, tz); err != nil {
			log.Fatal(err)
		}
		log.Println("importing orders")
		if err := importOrders(db, tz); err != nil {
			log.Fatal(err)
		}
	}
	// query customer table for earliest customer creation date
	startDate, err := getStartDate(db)
	if err != nil {
		log.Fatal(err)
	}
	// query customer table for latest customer creation date
	endDate, err := getEndDate(db)
	if err != nil {
		log.Fatal(err)
	}
	var cohortsRows [][]string
	headers := NewOrderedStringSet()
	log.Println("aggregating data")
	for {
		// iteratively query customer data set in intervals of seven days until date range exceeds latest customer creation date
		year, month, day := startDate.Date()
		gte := time.Date(year, month, day, 0, 0, 0, 0, tz)
		lt := gte.Add(time.Duration(7*24) * time.Hour)
		if rows, err := Query(db, "customers", []string{"id", "created"}, QueryOptions{
			OrderBy: "created",
			Asc:     true,
			Where:   fmt.Sprintf("created BETWEEN \"%s\" AND \"%s\"", gte.Format("2006-01-02T15:04:05Z"), lt.Format("2006-01-02T15:04:05Z")),
		}); err != nil {
			log.Fatal(err)
		} else {
			// generate cohort data from customers returned from query
			cohort, err := generateCohort(db, rows, fmt.Sprintf("%s-%s", gte.Format("01/02/2006"), gte.Add(time.Duration(6*24)*time.Hour).Format("01/02/2006")))
			if err != nil {
				log.Fatal(err)
			}
			// convert cohort struct data to rows comforming to expected format
			cohortsRows = append(cohortsRows, makeCohortRows(cohort, &headers)...)
		}
		startDate = &lt
		if startDate.After(*endDate) {
			break
		}
	}
	// append header row to cohort data rows
	cohortsRows = append([][]string{headers.Values()}, cohortsRows...)
	var output io.Writer
	// optionally set output to stdout or to target output file
	if *stdoutMode {
		output = os.Stdout
	} else {
		if outputFile, err := os.Create(*outputPath); err != nil {
			log.Fatal(err)
		} else {
			output = outputFile
		}
	}
	// write cohort csv data to target
	writeCohortRows(output, cohortsRows)
	log.Println("done")
}
