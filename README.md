# castmedia
An [icecast](https://icecast.org/) inspired media server that currently supports MP3 and AAC audio streams.

It's still in early development and any use in production is **highly** discouraged.

## Features

- Compatiblity with icecast protocol for source and listeners.
- Icecast metadata updates and broadcast.
- Configuring server resources via a single configuration file.
- JSON API for administration and server/mount/user stats.
- Updates without downtime (TLS is supported under Linux).
- Possibility to use YellowPages directories


Documentation can be found [here](./docs/getting_started.md).


## Todo

- Move YellowPages to server #11
- Add per mount configuration
- Add support for HLS
