## What is CastMedia?

CastMedia is an icecast media server that let's you broadcast media stream over internet. CastMedia currently supports MP3 media streams.

While CastMedia is a server that let's you broadcast media stream to clients. You still need an icecast source that provide media for the server. You can make your own client using [libshout](https://gitlab.xiph.org/xiph/icecast-libshout/) or use tools such as ffmpeg.

## Platforms

This should work on any Unix platform. But currently tested and supported:
- Linux
- Freebsd

## Installation

You can either install castmedia from the release page where prebuilt binaries are provided [here](https://codeberg.org/zesty/castmedia/releases).


Or you can build it yourself provided that already have the rust toolchain already installed in your system:

```
git clone https://codeberg.org/zesty/castmedia
cd castmedia
cargo build --release
```

## Pages

- [Introduction](./intro.md)
- [Configuration file](./config.md)
- [API](./api.md)
- [Relaying](./relaying.md)
- [Migration and zero-downtime](./migration.md)
