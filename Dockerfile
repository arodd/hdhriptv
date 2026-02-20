# syntax=docker/dockerfile:1.7

FROM alpine:3.23 AS runtime
RUN set -eux; \
	ALPINE_BRANCH="$(cut -d. -f1,2 /etc/alpine-release)"; \
	printf '%s\n' \
		"https://dl-cdn.alpinelinux.org/alpine/v${ALPINE_BRANCH}/main" \
		"https://dl-cdn.alpinelinux.org/alpine/v${ALPINE_BRANCH}/community" \
		> /etc/apk/repositories; \
	apk add --no-cache ca-certificates ffmpeg ttf-dejavu tzdata; \
	update-ca-certificates

WORKDIR /app
ARG TARGETARCH
COPY dist/hdhriptv-linux-${TARGETARCH} /usr/local/bin/hdhriptv

VOLUME ["/data"]
EXPOSE 5004/tcp 80/tcp 65001/udp 1900/udp

ENTRYPOINT ["/usr/local/bin/hdhriptv"]
