FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /controlplane ./cmd/controlplane
RUN test -d /src/runs/latest && test -n "$(find /src/runs/latest -name report.json -print -quit)"

FROM gcr.io/distroless/static:nonroot
COPY --from=build /controlplane /controlplane
COPY --from=build --chown=nonroot:nonroot /src/runs/latest /runs
ENV PORT=8080
ENV RUNS_DIR=/runs
EXPOSE 8080
ENTRYPOINT ["/controlplane"]
