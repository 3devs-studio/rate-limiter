FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/ratelimiter ./cmd/ratelimiter \
 && CGO_ENABLED=0 go build -o /out/fake-upstream ./examples/fake-upstream \
 && CGO_ENABLED=0 go build -o /out/loadgen ./examples/loadgen

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /out/ /usr/local/bin/
COPY config.example.yaml /etc/ratelimiter/config.yaml
EXPOSE 8080
ENTRYPOINT ["ratelimiter"]
CMD ["-config", "/etc/ratelimiter/config.yaml"]
