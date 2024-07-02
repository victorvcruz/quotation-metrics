FROM golang:1.22 AS build

ARG GITHUB_TOKEN
ENV CGO_ENABLED=0 GO111MODULE=on GOOS=linux

WORKDIR /app

COPY ../.. ./

RUN cd ./cmd \
    && go build -a -installsuffix cgo -ldflags="-s -w" -o /server

FROM gcr.io/distroless/static-debian11

WORKDIR /

COPY --from=build /server /server

EXPOSE 8080

ENTRYPOINT ["/server"]