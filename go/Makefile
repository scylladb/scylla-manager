GO_VERSION = $(shell cat ../.go-version)
GO_URL    := https://storage.googleapis.com/golang/go$(GO_VERSION).linux-amd64.tar.gz
GO_BUNDLE := go$(GO_VERSION).linux-amd64.tar.gz

CURL := curl -sSq

all: $(GO_BUNDLE)

$(GO_BUNDLE):
	@echo "==> Downloading Go $(GO_VERSION)..."
	@$(CURL) $(GO_URL) -o $(GO_BUNDLE)
	@[ "`sha256sum $(GO_BUNDLE) | cut -c1-64`" == "`$(CURL) $(GO_URL).sha256`" ]
	@ln -sf $(GO_BUNDLE) latest

.PHONY: install
install: $(GO_BUNDLE)
install: TARGET=$(shell dirname $(GOROOT))/go$(GO_VERSION).linux-amd64
install:
ifndef GOROOT
	$(error set GOROOT environment variable as '/PATH/TO/GO/SDKS/latest')
endif
	@echo "==> Installing Go $(GO_VERSION) to $(TARGET)..."
	@[ -d $(TARGET) ] || (mkdir -p $(TARGET) && tar -xzf $(GO_BUNDLE) -C $(TARGET))
	@rm -f `dirname $(TARGET)`/latest
	@ln -sf $(TARGET)/go `dirname $(TARGET)`/latest

.PHONY: clean
clean:
	@rm -f latest go*
