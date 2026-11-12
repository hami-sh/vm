FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /controlplane ./cmd/controlplane

FROM gcr.io/distroless/static:nonroot
COPY --from=build /controlplane /controlplane
ENV PORT=8080
EXPOSE 8080
ENTRYPOINT ["/controlplane"]
