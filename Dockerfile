# Shopping Sessions: Rust pipeline.
# Stage 1 compiles the program (needs network, allowed at build time, limit 10 min).
# Stage 2 is a small image that only holds the binary; the scorer runs it with no network.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
# The scorer runs the container as uid 1000; give that uid a name and a home.
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/release/pipeline /app/pipeline
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh /app/pipeline
WORKDIR /app
CMD ["/app/run.sh"]
