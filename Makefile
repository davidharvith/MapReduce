.PHONY: all build test clean bench bench-sweep word-count-demo

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

bench-sweep: build
	chmod +x benchmarks/run_sweep.sh
	./benchmarks/run_sweep.sh

word-count-demo: build
	./$(BUILD_DIR)/word_count examples/sample.txt --top 15 --threads 4

clean:
	rm -rf $(BUILD_DIR)
