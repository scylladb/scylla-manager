FROM alpine:3.24.3

# Binaries are static and bind-mounted at runtime; only TZ data is needed.
RUN apk add --no-cache tzdata

ENV TZ="America/New_York"
