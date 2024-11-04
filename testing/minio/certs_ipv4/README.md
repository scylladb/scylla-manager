This is just a testing certificate used to validate and show the possibility of setting custom CA certificate for 
S3 compatible storage - Minio.

Password for rootCA.pem: "Test"

To regenerate the certificate:
➜  certs_ipv4 git:(master) ✗ openssl x509 -req -in new_request.csr -CA CAs/rootCA.pem -CAkey CAs/rootCA.key -CAcreateserial -out public.crt -days 3650 -extfile <(printf "subjectAltName=IP:192.168.200.99")
