BINARY_NAME=nats-bench
CMD_DIR=cmd
BUILD_DIR=build
OS := $(shell uname)

ifeq ($(OS),Darwin)
	    SED_CMD = sed -i ""
    else
	    SED_CMD = sed -i
    endif

debug:
		go run $(CMD_DIR)/$(BINARY_NAME)/*.go

build:
		CGO_ENABLED=0 go build -C $(CMD_DIR)/$(BINARY_NAME)  -o ../../$(BUILD_DIR)/$(BINARY_NAME) && \
		cd $(BUILD_DIR) && tar -czf $(BINARY_NAME).tar.gz ./*

test:
		go test ./...

build-linux:
	GOOS=linux GOARCH=amd64 go build -C $(CMD_DIR)/$(BINARY_NAME) -o ../../$(BUILD_DIR)/$(BINARY_NAME)-linux-amd64 && \
	cd $(BUILD_DIR) && tar -czf $(BINARY_NAME)-linux-amd64.tar.gz ./*

run: build
		./$(BUILD_DIR)/$(BINARY_NAME) --config $(CONFIG_PATH)
clean:
		rm -rf $(BUILD_DIR)
