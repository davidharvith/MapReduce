.PHONY: all build test clean bench

BUILD_DIR ?= build

all: build

build:
	cmake -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=Release
	cmake --build $(BUILD_DIR) -j

test: build
	ctest --test-dir $(BUILD_DIR) --output-on-failure

bench: build
	./$(BUILD_DIR)/bench --threads 1
	./$(BUILD_DIR)/bench --threads 2
	./$(BUILD_DIR)/bench --threads 4
	./$(BUILD_DIR)/bench --threads 8

clean:
	rm -rf $(BUILD_DIR)
