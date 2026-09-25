FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/forger ./cmd/forger

FROM alpine:3.20
RUN adduser -D -u 10001 forger
COPY --from=build /out/forger /usr/local/bin/forger
RUN mkdir -p /data && chown forger:forger /data
USER forger
ENTRYPOINT ["forger"]
