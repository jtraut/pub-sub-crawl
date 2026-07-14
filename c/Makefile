CC := gcc
CFLAGS := -std=c11 -D_POSIX_C_SOURCE=200809L -Wall -Wextra -pthread -Iinclude
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

.PHONY: all clean wsl-build wsl-clean wsl-run

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(OBJS) -o $@ $(LDFLAGS)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

clean:
	rm -rf $(BUILD_DIR) $(TARGET)

# This project needs real POSIX headers (sys/socket.h, sys/un.h, a
# SIGPIPE-having signal.h) for its AF_UNIX socket code -- Windows'
# MinGW-w64 gcc doesn't provide them, no matter what CFLAGS you pass, so
# `make`/`make clean` above only work as-is on an actual POSIX toolchain.
# These targets shell out to WSL (must have a distro installed, e.g.
# Ubuntu) and run the real build/run there against this same source tree.
WSL_DIR := $(shell wsl.exe wslpath -a "$(CURDIR)")

wsl-build:
	wsl.exe bash -lc "cd '$(WSL_DIR)' && make"

wsl-clean:
	wsl.exe bash -lc "cd '$(WSL_DIR)' && make clean"

wsl-run: wsl-build
	wsl.exe bash -lc "cd '$(WSL_DIR)' && ./radio_sim"
