FROM messense/rust-musl-cross:x86_64-musl AS builder
WORKDIR /usr/src/
COPY . ./
RUN cargo build --release --target=x86_64-unknown-linux-musl

# Bundle Stage
FROM alpine
RUN apk add --update openssl bash
COPY --from=builder /usr/src/target/x86_64-unknown-linux-musl/release/piper /app/piper
COPY --from=builder /usr/src/conf /conf
# USER 1000
WORKDIR /conf
EXPOSE 8000
CMD ["/app/piper", "-p", "/conf/pipeline.conf", "-l", "/conf/lookup.json", "--port", "8000"]
