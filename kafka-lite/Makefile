CXX ?= c++
CXXFLAGS ?= -std=c++20 -Wall -Wextra -Iinclude
LDFLAGS ?=

BUILD_DIR ?= build
OBJS := $(BUILD_DIR)/log.o $(BUILD_DIR)/broker.o
FLAGS_STAMP := $(BUILD_DIR)/.cxxflags

.PHONY: all demo scenario test test-san run run-scenario clean FORCE

all: demo test

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

# Rebuild .o files when CXX / CXXFLAGS / LDFLAGS change (e.g. after test-san).
$(FLAGS_STAMP): FORCE | $(BUILD_DIR)
	@printf '%s\n' '$(CXX)' '$(CXXFLAGS)' '$(LDFLAGS)' > $@.tmp
	@if cmp -s $@ $@.tmp 2>/dev/null; then rm -f $@.tmp; else mv $@.tmp $@; fi

$(BUILD_DIR)/log.o: src/log.cpp $(FLAGS_STAMP)
	$(CXX) $(CXXFLAGS) -c src/log.cpp -o $@

$(BUILD_DIR)/broker.o: src/broker.cpp $(FLAGS_STAMP)
	$(CXX) $(CXXFLAGS) -c src/broker.cpp -o $@

demo: $(OBJS) examples/demo.cpp
	$(CXX) $(CXXFLAGS) examples/demo.cpp $(OBJS) -o $(BUILD_DIR)/demo $(LDFLAGS)

scenario: $(OBJS) examples/scenario.cpp
	$(CXX) $(CXXFLAGS) examples/scenario.cpp $(OBJS) -o $(BUILD_DIR)/scenario $(LDFLAGS)

test: $(OBJS) tests/test_kafka.cpp
	$(CXX) $(CXXFLAGS) tests/test_kafka.cpp $(OBJS) -o $(BUILD_DIR)/test_kafka $(LDFLAGS)
	$(BUILD_DIR)/test_kafka

# Keep sanitizer objects in build-san so they never mix with a normal build/.
test-san:
	$(MAKE) test BUILD_DIR=build-san \
		CXXFLAGS="$(CXXFLAGS) -O1 -fsanitize=address,undefined -fno-omit-frame-pointer" \
		LDFLAGS="-fsanitize=address,undefined"

run: demo
	$(BUILD_DIR)/demo

run-scenario: scenario
	$(BUILD_DIR)/scenario

clean:
	rm -rf build build-san

FORCE:
