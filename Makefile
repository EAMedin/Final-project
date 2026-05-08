CXX      = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -pthread -O2
TARGET   = scheduler_os
SRC      = scheduler_os.cpp

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(SRC)

clean:
	rm -f $(TARGET)

.PHONY: clean
