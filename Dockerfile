FROM messense/rust-musl-cross:x86_64-musl AS builder
WORKDIR /usr/src/
COPY . ./
RUN cargo build --release --target=x86_64-unknown-linux-musl --package=standalone

FROM alpine
COPY --from=builder /usr/src/target/x86_64-unknown-linux-musl/release/piper /app/piper
COPY --from=builder /usr/src/conf /conf
COPY --from=builder /usr/src/test-data /conf/test-data
WORKDIR /conf
EXPOSE 8000
CMD ["/app/piper", "-p", "/conf/pipeline.conf", "-l", "/conf/lookup.json", "--port", "8000"]
