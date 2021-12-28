FROM ubuntu

# Install 3rd party tools
RUN apt-get update && apt-get install -y tzdata \
    && rm -rf /var/lib/apt/lists/*

ENV TZ="America/New_York"
