FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd cmd
COPY pkg pkg
RUN CGO_ENABLED=0 go build -o /minio-cosi-driver ./cmd/minio-cosi-driver

FROM gcr.io/distroless/static:nonroot
LABEL description="MinIO COSI driver (release-0.2 / v1alpha1), lazedo fork"
COPY --from=build /minio-cosi-driver /minio-cosi-driver
ENTRYPOINT ["/minio-cosi-driver"]
