# Build stage
FROM golang:1.21-alpine AS builder

# Set working directory
WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o zugkraftdb ./cmd/client

# Final stage
FROM alpine:latest

# Install CA certificates
RUN apk --no-cache add ca-certificates

# Set working directory
WORKDIR /app

# Copy the binary from builder
COPY --from=builder /app/zugkraftdb .

# Copy config file
COPY config/config.example.yaml ./config.yaml

# Expose the application port
EXPOSE 8080

# Command to run the application
CMD ["./zugkraftdb"]