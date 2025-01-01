
# YellowPages directories

A YP (Yellow Pages) directory is a listing of broadcast streams.

There are multiple known YP directories such as the one hosted by the icecast project at: http://dir.xiph.org/ or even popular internet radio curators such as [internet radio](https://www.internet-radio.com/) at: http://icecast-yp.internet-radio.com/

# YP in castmedia

Castmediayp is a YP client that sends broadcast stream updates to YP directories.

To use it you must first build it if needed:

```
cargo build --release --package castmediayp
```

To be able to read updates from castmedia server, you must configure castmedia to accept connections coming from castmediayp by adding an account with yp role in the account section:
```
account:
  ypclient:
    role: yp
    pass: 0$pass
```

Next we configure castmediayp:
```
# castmedia server we will want to send broadcast stream updates to
server: http://ypclient:pass@127.0.0.1:39203
# The public address of the radio where listeners will access stream
# if there are directories before mount they must also be specified (ex: https://my.radio/channel/)
public_server: https://my.radio/
# Website of radio (can be any valid url)
url: https://my.radio/
# Timeout for connections
timeout: 20000
# YP directories entries
directories:
  - yp_url: http://icecast-yp.internet-radio.com
    # Periodic heartbeat timeout when idle
    # Must consult YP diectory recommendation for this value
    timeout: 15000
# Where to save castmediayp state
state: yp_state.json
```

And we can then run castmediayp:
```
castmediayp configyp.yaml
```

# Caveats
- Currently listeners count is not sent to YP directories (and will not be implemented if there is no necessity to).
- Each YP directory may behave differently, currently only two are tested:
    - http://dir.xiph.org
    - http://icecast-yp.internet-radio.com
