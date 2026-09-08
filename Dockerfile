# Build both binaries once and pick one at runtime with the CMD, so the image
# is built a single time and shared by the queued and worker services.
FROM golang:1.25 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ENV CGO_ENABLED=0
RUN go build -trimpath -ldflags="-s -w" -o /out/queued ./cmd/queued && \
    go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker && \
    go build -trimpath -ldflags="-s -w" -o /out/enqueue ./cmd/enqueue

FROM gcr.io/distroless/static:nonroot

COPY --from=build /out/queued /queued
COPY --from=build /out/worker /worker
COPY --from=build /out/enqueue /enqueue

COPY migrations /migrations
COPY web /web

USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/queued"]
