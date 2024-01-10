# This builds in Linux. Unfortunately Docker's overlay fs doesn't seem to support file cloning.

FROM alpine

RUN apk add rustup
RUN rustup-init -y --profile minimal

RUN apk add gcc musl-dev

WORKDIR /app

COPY Cargo.* .
COPY src src
COPY benches benches
COPY manifest.sql .

RUN --mount=type=cache,target=/root/.cargo/registry \
	--mount=type=cache,target=/root/.cargo/git \
	--mount=type=cache,target=/app/target \
	. ~/.cargo/env && RUST_BACKTRACE=1 cargo test
