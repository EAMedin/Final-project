CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -pedantic -pthread
TARGET = scheduler_os
SOURCE = scheduler_os.cpp

all: $(TARGET)

$(TARGET): $(SOURCE)
	$(CXX) $(CXXFLAGS) $(SOURCE) -o $(TARGET)

clean:
	rm -f $(TARGET)
