.PHONY: all clean

PROJECT_PATH = github.com/utmhikari/daggre

# for binaries
BIN_ROOT=bin
CGO_ENABLED=0
GOOS=windows
GOARCH=amd64

all: app

app:
	@echo "make daggre app..."
	mkdir -p $(BIN_ROOT)
	go build -o $(BIN_ROOT) daggre.go

clean:
	@echo "clean binaries"
	rm -rf $(BIN_ROOT)
