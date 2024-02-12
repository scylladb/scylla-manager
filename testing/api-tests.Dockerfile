FROM golang:1.22.0-alpine

RUN apk --no-cache add make && apk add bash

RUN mkdir /scylla-manager
