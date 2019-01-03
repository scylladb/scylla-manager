SUBJECT := /C=US/ST=Scylla/L=Scylla/O=Scylla/OU=Scylla/CN=scylla.com

db.crt: db.key
	openssl req -new -x509 -sha512 -subj $(SUBJECT) -key db.key -out db.crt -days 365

db.key:
	openssl genrsa -out db.key 4096 &> /dev/null
