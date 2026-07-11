CC := gcc
CFLAGS := -std=c11 -Wall -Wextra -pthread -Iinclude
LDFLAGS := -pthread

SRC_DIR := src
BUILD_DIR := build

ifeq ($(OS),Windows_NT)
	EXE := .exe
else
	EXE :=
endif

TARGET := radio_sim$(EXE)

SRCS := $(wildcard $(SRC_DIR)/*.c)
OBJS := $(patsubst $(SRC_DIR)/%.c,$(BUILD_DIR)/%.o,$(SRCS))

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(OBJS) -o $@ $(LDFLAGS)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

clean:
	rm -rf $(BUILD_DIR) $(TARGET)
