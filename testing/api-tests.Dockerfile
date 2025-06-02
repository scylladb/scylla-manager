FROM golang:1.23.2-alpine

RUN apk --no-cache add make && apk add bash

RUN mkdir /scylla-manager
