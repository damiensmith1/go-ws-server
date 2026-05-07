# syntax=docker/dockerfile:1.7

FROM golang:1.24-alpine AS build

WORKDIR /src
RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Static, stripped binary. CGO_ENABLED=0 lets us copy into a distroless
# image with no libc.
RUN CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags="-s -w" -o /out/go-ws-server ./


FROM gcr.io/distroless/static:nonroot

USER nonroot:nonroot
COPY --from=build /out/go-ws-server /go-ws-server

EXPOSE 8080
ENTRYPOINT ["/go-ws-server"]
