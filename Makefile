CXX = g++
CXXFLAGS = -std=c++20 -Wall

TARGET = hello-world
SRC = hello-world.cpp

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) $(SRC) -o $(TARGET)

run: $(TARGET)
	./$(TARGET)

ABSL_PREFIX = /opt/homebrew
ABSL_LIBS = -labsl_raw_hash_set -labsl_hash -labsl_city -labsl_throw_delegate -labsl_raw_logging_internal

clean:
	rm -f $(TARGET) algorithms/map_test

.PHONY: kafka map_test
kafka:
	$(MAKE) -C kafka-lite

map_test: algorithms/map_test.cpp
	c++ -std=c++20 $< -o algorithms/map_test \
		-I$(ABSL_PREFIX)/include -L$(ABSL_PREFIX)/lib \
		-Wl,-rpath,$(ABSL_PREFIX)/lib $(ABSL_LIBS)
	./algorithms/map_test
