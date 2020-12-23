#!/usr/bin/env bash
#
# Copyright (C) 2017 ScyllaDB
#

set -eu -o pipefail

# Generate keys for Certificate Authority (ca), Database node (db), and Client (cl)
openssl genrsa -out ca.key 4096 &> /dev/null
openssl genrsa -out db.key 4096 &> /dev/null
openssl genrsa -out cl.key 4096 &> /dev/null

# Generate certificate for Certificate Authority (ca)
openssl req -x509 -new -nodes -key ca.key -days 3650 -config ca.cfg -out ca.crt
# Generate certificate for Database node (db)
openssl req -new -key db.key -out db.csr -config db.cfg
openssl x509 -req -in db.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out db.crt -days 365
# Generate certificate for Client (cl)
openssl req -new -key cl.key -out cl.csr -config cl.cfg
openssl x509 -req -in cl.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out cl.crt -days 365
