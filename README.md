# castmedia
An [icecast](https://icecast.org/) inspired media server that currently supports MP3 audio streams.

It's still in early development and any use in production is **highly** discouraged.

## Features

- Http compatible icecast protocol for source and listeners.
- Icecast metadata updates and broadcast.
- Configuring server resources via a single configuration file.
- Configuring accounts for access.
- Json API for administration and server/mount/user stats.
- Updates without downtime (without Tls).


Documentation can be found [here](./docs/getting_started.md).


## Todo

- Add replication
- Add per mount configuration
- Add ogg stream support, for this we should have a way to parse initial stream header from source,
which currently there is no way todo with symphonia
