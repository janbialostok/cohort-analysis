## Buildling

All dependencies for this project are included as part of the vendor directory so building is very simple.

Run from the root of the project:

```sh
$ go build
```

## Running

A convenience script is included as part of the project. To run with default options:

```sh
$ sh ./exec.sh
```

To run with custom options:

```sh
$ ./cohort-analysis
```

Options available are:

* -db (defaults to "./cohort-analysis.db") specifies the db file in which SQL data should be stored
* -customers (defaults to "./data/customers.csv") specifies the path to the customer data
* -orders (defaults to "./data/orders.csv") specifies the path to the order data
* -datetimeLayout (defaults to "2006-01-02 15:04:05 UTC") specifies the layout of datetime
* -timezone (defaults to "UTC") specifies the timezone the UTC defined datetimes should be stored in (see golang timezone locations for comaptible list)
* -import (defaults to "true") specifies if import of data should run
* -output (defaults to ./results.csv") specifies the file path for the results output ignored if stdout mode is enabled
* -stdout, (defaults to false) specifies that the output csv should be written to stdout

## Building and Running with Docker

First build the dockerfile which will also run an import of the data

```sh
$ docker build -t cohort-analysis .
```

Next export the output CSV to your host directory

```sh
$ docker run cohort-analysis cat output/results.csv > path/to/host/file
```

You can also bash into the container if you want to re-run the process with additional commands

```sh
$ docker run -it cohort-analysis bash
$ ./cohort-analysis -import false
```

## Testing

You can run unit test only with:

```sh
$ go test -short
```

You can run all test with:

```sh
$ go test
```

