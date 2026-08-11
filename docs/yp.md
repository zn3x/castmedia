# YellowPages directories

A YP (Yellow Pages) directory is a listing of broadcast streams.

There are multiple known YP directories such as the one hosted by the icecast project at: http://dir.xiph.org/ or even popular internet radio curators such as [internet radio](https://www.internet-radio.com/) at: http://icecast-yp.internet-radio.com/

# YP in castmedia

Castmedia is a YP client that sends broadcast stream updates to YP directories. When a source connects, the stream is registered on every configured directory, kept alive with periodic touches, updated when the stream metadata changes, and removed once the source disconnects.

To use it you only need to enable the `yellow_pages` section in the castmedia server configuration:

```
yellow_pages:
  enabled: true
  # Public url where clients will be able to reach streams.
  # The stream mount is appended to the end of this url.
  public_server: https://my.radio/
  # Radio website mainpage (can be any valid url)
  url: https://my.radio/
  # YP directories entries
  directories:
    - yp_url: http://icecast-yp.internet-radio.com
      # Timeout in millis to send requests to the YP directory
      timeout: 15000
  # Where to store the yellow pages state info
  state: yp_state
```

For each mounted stream, a registration request is sent to every configured directory carrying the stream ICY properties (name, genre, description, bitrate, content type, ...) and the `Ice-Public` flag sent by the source, so a stream mounted with `Ice-Public: 0` is hidden from the directory's public listing. The directory replies with a `sid` and a `touchfreq`; the server then sends touches at that interval and whenever the metadata (e.g. the song title) changes. When the source disconnects, the listing is removed from the directories.

State is persisted per-mount inside the `state` directory so a stream registered before a server restart or migration is not registered a second time.

# Caveats
- Currently listeners count is not sent to YP directories (and will not be implemented if there is no necessity to).
- Each YP directory may behave differently, currently only two are tested:
    - http://dir.xiph.org
    - http://icecast-yp.internet-radio.com
