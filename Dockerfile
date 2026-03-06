FROM golang:1.24-alpine AS builder

WORKDIR /app

ARG TARGET=server

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download all dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o /appbin ./cmd/${TARGET}

# Final stage
FROM alpine:latest

WORKDIR /

# Copy the Pre-built binary file from the previous stage
COPY --from=builder /appbin /appbin

# Expose port 8080 to the outside world
EXPOSE 8080

# Command to run the executable
CMD ["/appbin"]
