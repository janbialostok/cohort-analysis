FROM golang:latest
LABEL maintainer="Jan Bialostok <janbialostok@gmail.com>"
WORKDIR /src

COPY ./ ./

RUN go build

RUN mkdir ./output

RUN ./cohort-analysis -output ./output/results.csv

VOLUME ./output

