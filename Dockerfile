FROM rust:1.44 as builder

# ! needed for rustls to compile for the x86_64-unknown-linux-musl target.
RUN apt update \
    && apt install -y clang

WORKDIR /usr/src/uploader
COPY Cargo.toml Cargo.lock ./

# creates dummy main to compile dependencies against.
# this prevents local builds from having to build everything in the event
# there are no changes to the dependencies.
RUN mkdir src \
    && echo "fn main() {print!(\"Dummy Uploader\");}" > src/main.rs

# ! needed to target `scratch` image
RUN rustup target install x86_64-unknown-linux-musl

# build dependencies
RUN cargo build --release

# build Uploader
COPY src ./src
RUN TARGET_CC=clang cargo install --target x86_64-unknown-linux-musl --path .

# * Create the release image.
FROM scratch
COPY --from=builder /usr/local/cargo/bin/uploader /usr/local/bin/uploader
USER 1709
ENTRYPOINT ["uploader"]
CMD ["--help"]
