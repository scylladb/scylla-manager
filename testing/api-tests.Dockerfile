FROM golang:1.25.1-alpine

RUN apk --no-cache add make && apk add bash

RUN mkdir /scylla-manager
