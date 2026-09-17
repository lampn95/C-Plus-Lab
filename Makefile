CXX = g++
CXXFLAGS = -std=c++17 -Wall

TARGET = hello-world
SRC = hello-world.cpp

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) $(SRC) -o $(TARGET)

run: $(TARGET)
	./$(TARGET)

clean:
	rm -f $(TARGET)

.PHONY: kafka
kafka:
	$(MAKE) -C kafka-lite