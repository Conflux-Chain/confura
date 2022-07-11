# builder image
FROM golang:1.16 AS builder
RUN mkdir /build
WORKDIR /build
COPY go.mod go.sum ./
# you may use `GOPROXY` to speedup in Mainland China.
# RUN  GOPROXY=https://goproxy.cn,direct go mod download
RUN go mod download
COPY . .
RUN go build -o confura .

# final target image for multi-stage builds
FROM alpine:3.16
RUN apk --no-cache add ca-certificates
RUN mkdir /lib64 && ln -s /lib/libc.musl-x86_64.so.1 /lib64/ld-linux-x86-64.so.2
COPY --from=builder /build/confura .
COPY ./config/config.yml ./config.yml
ENTRYPOINT [ "./confura" ]
CMD [ "--help" ]