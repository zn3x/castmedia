
# API

There are two different APIs existing in castmedia.

## Admin API

An interface which require authentication.

The two following endpoints can be accessed either by an administrator or a source user that owns the mount

- `/admin/metadata`: Update metadata for a mount

```
curl -u "source:pass" -v "http://127.0.0.1:9100/admin/metadata?mount=/stream&mode=updinfo&song=Best&url=example.com"
*   Trying 127.0.0.1:9100...
* Connected to 127.0.0.1 (127.0.0.1) port 9100
* Server auth using Basic with user 'source'
> GET /admin/metadata?mount=/stream&mode=updinfo&song=Best&url=example.com HTTP/1.1
> Host: 127.0.0.1:9100
> Authorization: Basic c291cmNlOnBhc3M=
> User-Agent: curl/8.7.1
> Accept: */*
>
* Request completely sent off
< HTTP/1.1 200 OK
< Connection: close
< Server: castmedia 0.1.0
< Date: Sat,  6 Jul 2024 15:41:13 GMT
< Cache-Control: no-cache, no-store
< Expires: Mon, 26 Jul 1997 05:00:00 GMT
< Pragma: no-cache
< Access-Control-Allow-Origin: *
<
* Closing connection
```

- `/admin/fallbacks`: Changing fallback for a mount. One particularity of setting a fallback is that castmedia will not check if the fallback mount exists or not, it will only be evaluated when fallbacking to other source is needed, if the fallback source with the given mount was not found, all users would be dropped.

```
curl -u "source:pass" -v "http://127.0.0.1:9100/admin/fallbacks?mount=/stream&fallback=/stream1"
*   Trying 127.0.0.1:9100...
* Connected to 127.0.0.1 (127.0.0.1) port 9100
* Server auth using Basic with user 'source'
> GET /admin/fallbacks?mount=/stream&fallback=/stream1 HTTP/1.1
> Host: 127.0.0.1:9100
> Authorization: Basic c291cmNlOnBhc3M=
> User-Agent: curl/8.7.1
> Accept: */*
>
* Request completely sent off
< HTTP/1.1 200 OK
< Connection: close
< Server: castmedia 0.1.0
< Date: Sat,  6 Jul 2024 15:43:06 GMT
< Cache-Control: no-cache, no-store
< Expires: Mon, 26 Jul 1997 05:00:00 GMT
< Pragma: no-cache
< Access-Control-Allow-Origin: *
<
* Closing connection
```

The following endpoints are only allowed by an administrator

- `/admin/stats`: General server stats

```
curl -u "admin:pass" -v "http://127.0.0.1:9100/admin/stats"
*   Trying 127.0.0.1:9100...
* Connected to 127.0.0.1 (127.0.0.1) port 9100
* Server auth using Basic with user 'admin'
> GET /admin/stats HTTP/1.1
> Host: 127.0.0.1:9100
> Authorization: Basic YWRtaW46cGFzcw==
> User-Agent: curl/8.7.1
> Accept: */*
>
* Request completely sent off
< HTTP/1.1 200 OK
< Connection: close
< Content-Length: 327
< Content-Type: application/json; charset=utf-8
< Server: castmedia 0.1.0
< Date: Sat,  6 Jul 2024 15:45:58 GMT
< Cache-Control: no-cache, no-store
< Expires: Mon, 26 Jul 1997 05:00:00 GMT
< Pragma: no-cache
< Access-Control-Allow-Origin: *
<
* Closing connection
{
  "active_clients": 3, # Number of total active client connections
  "active_listeners": 0, # Number of total active listening clients to sources
  "active_relay": 0, # Number of total active outbound connections to master server
  "active_relay_streams": 0, # Number of active streams that are relayed
  "active_sources": 2, # Number of total active sources
  "admin_api_connections": 8, # Number of connections made to admin api (accumulating counter)
  "admin_api_connections_success": 10, # Number of admin api connections with successful authentication (accumulating counter)
  "api_connections": 3, # Number of public api connections (accumulating counter)
  "connections": 16, # Number of connections since startup (accumulating counter). This includes number of failed connections (max clients reached, invalid request, ... etc)
  "listener_connections": 0, # Number of connections to sources (accumulating counter)
  "peak_listeners": 0, # Number of peak listeners to sources
  "source_client_connections": 4, # Number of connections made by source clients (accumulating counter)
  "source_relay_connections": 0, # Number of outbound connections made to master server (accumulating counter)
  "start_time": 1720272275 # Server startup time as a utc timestamp
}
```

- `/admin/listmounts`: Show mounts with related stats

```
curl -u "admin:pass" -v "http://127.0.0.1:9100/admin/listmounts"
*   Trying 127.0.0.1:9100...
* Connected to 127.0.0.1 (127.0.0.1) port 9100
* Server auth using Basic with user 'admin'
> GET /admin/listmounts HTTP/1.1
> Host: 127.0.0.1:9100
> Authorization: Basic YWRtaW46cGFzcw==
> User-Agent: curl/8.7.1
> Accept: */*
>
* Request completely sent off
< HTTP/1.1 200 OK
< Connection: close
< Content-Length: 704
< Content-Type: application/json; charset=utf-8
< Server: castmedia 0.1.0
< Date: Sat,  6 Jul 2024 14:40:13 GMT
< Cache-Control: no-cache, no-store
< Expires: Mon, 26 Jul 1997 05:00:00 GMT
< Pragma: no-cache
< Access-Control-Allow-Origin: *
<
* Closing connection
{
  "/stream": {
    "fallback": "/stream1",
    "metadata": {
      "title": "Best",
      "url": "example.com"
    },
    "properties": {
      "bitrate": null,
      "content_type": "audio/mpeg",
      "description": null,
      "genre": null,
      "name": null,
      "public": false,
      "uagent": "Lavf/60.16.100",
      "url": null
    },
    "source_username": "source",
    "stats": {
      "active_listeners": 0, # Number of active listeners for source
      "bytes_read": 930562, # Number of media bytes read from source client
      "bytes_sent": 0, # Number of media bytes sent to all listeners of source
      "peak_listeners": 0, # Peak number of listeners for source
      "start_time": 1720276806 # Time when source last mounted mountpoint as utc timestamp
    }
  },
  "/stream1": {
    "fallback": null,
    "metadata": {
      "title": "",
      "url": ""
    },
    "properties": {
      "bitrate": null,
      "content_type": "audio/mpeg",
      "description": null,
      "genre": null,
      "name": null,
      "public": false,
      "uagent": "Lavf/60.16.100",
      "url": null
    },
    "source_username": "source",
    "stats": {
      "active_listeners": 0,
      "bytes_read": 917605,
      "bytes_sent": 0,
      "peak_listeners": 0,
      "start_time": 1720276807
    }
  }
}
```

- `/admin/moveclients`: Move clients from one source to another

```
curl -u "admin:pass" -v "http://127.0.0.1:9100/admin/moveclients?mount=/stream&destination=/stream1"
*   Trying 127.0.0.1:9100...
* Connected to 127.0.0.1 (127.0.0.1) port 9100
* Server auth using Basic with user 'admin'
> GET /admin/moveclients?mount=/stream&destination=/stream1 HTTP/1.1
> Host: 127.0.0.1:9100
> Authorization: Basic YWRtaW46cGFzcw==
> User-Agent: curl/8.7.1
> Accept: */*
>
* Request completely sent off
< HTTP/1.1 200 OK
< Connection: close
< Server: castmedia 0.1.0
< Date: Sat,  6 Jul 2024 14:47:38 GMT
< Cache-Control: no-cache, no-store
< Expires: Mon, 26 Jul 1997 05:00:00 GMT
< Pragma: no-cache
< Access-Control-Allow-Origin: *
<
* Closing connection
```

- `/admin/killsource`: Closing a source

```
curl -u "admin:pass" -v "http://127.0.0.1:9100/admin/killsource?mount=/stream"
*   Trying 127.0.0.1:9100...
* Connected to 127.0.0.1 (127.0.0.1) port 9100
* Server auth using Basic with user 'admin'
> GET /admin/killsource?mount=/stream1 HTTP/1.1
> Host: 127.0.0.1:9100
> Authorization: Basic YWRtaW46cGFzcw==
> User-Agent: curl/8.7.1
> Accept: */*
>
* Request completely sent off
< HTTP/1.1 200 OK
< Connection: close
< Server: castmedia 0.1.0
< Date: Sat,  6 Jul 2024 14:49:48 GMT
< Cache-Control: no-cache, no-store
< Expires: Mon, 26 Jul 1997 05:00:00 GMT
< Pragma: no-cache
< Access-Control-Allow-Origin: *
<
* Closing connection
```

- `/admin/listclients`: List clients in a mount

```
curl -u "admin:pass" -v "http://127.0.0.1:9100/admin/listclients?mount=/stream"
*   Trying 127.0.0.1:9100...
* Connected to 127.0.0.1 (127.0.0.1) port 9100
* Server auth using Basic with user 'admin'
> GET /admin/listclients?mount=/stream HTTP/1.1
> Host: 127.0.0.1:9100
> Authorization: Basic YWRtaW46cGFzcw==
> User-Agent: curl/8.7.1
> Accept: */*
>
* Request completely sent off
< HTTP/1.1 200 OK
< Connection: close
< Transfer-Encoding: Chunked
< Content-Type: application/json; charset=utf-8
< Server: castmedia 0.1.0
< Date: Sat,  6 Jul 2024 14:52:24 GMT
< Cache-Control: no-cache, no-store
< Expires: Mon, 26 Jul 1997 05:00:00 GMT
< Pragma: no-cache
< Access-Control-Allow-Origin: *
<
* Closing connection
{
  "293ca182-e9fc-4aae-a1b1-16630264da6e": {
    "properties": {
      "addr": "10.0.2.2:47342",
      "metadata": true,
      "user_agent": "libmpv"
    },
    "stats": {
      "bytes_sent": 56457,
      "start_time": 1720277543
    }
  }
}
```

- `/admin/killclient`: Kill a client

```
curl -u "admin:pass" -v "http://127.0.0.1:9100/admin/killclient?mount=/stream&id=293ca182-e9fc-4aae-a1b1-16630264da6e"
*   Trying 127.0.0.1:9100...
* Connected to 127.0.0.1 (127.0.0.1) port 9100
* Server auth using Basic with user 'admin'
> GET /admin/killclient?mount=/stream&id=293ca182-e9fc-4aae-a1b1-16630264da6e HTTP/1.1
> Host: 127.0.0.1:9100
> Authorization: Basic YWRtaW46cGFzcw==
> User-Agent: curl/8.7.1
> Accept: */*
>
* Request completely sent off
< HTTP/1.1 200 OK
< Connection: close
< Server: castmedia 0.1.0
< Date: Sat,  6 Jul 2024 14:55:39 GMT
< Cache-Control: no-cache, no-store
< Expires: Mon, 26 Jul 1997 05:00:00 GMT
< Pragma: no-cache
< Access-Control-Allow-Origin: *
<
* Closing connection
```

- `/admin/shutdown`: Remotely stop castmedia


- `/admin/mountupdates`: Endpoint for relay communications, only a slave user is allowed

## Public API

This is a public API that can be accessed by any user trying to access them without any authentication.

- `/api/serverinfo`: Get server info

```
curl -v "http://127.0.0.1:9000/api/serverinfo"
*   Trying 127.0.0.1:9000...
* Connected to 127.0.0.1 (127.0.0.1) port 9000
> GET /api/serverinfo HTTP/1.1
> Host: 127.0.0.1:9000
> User-Agent: curl/8.7.1
> Accept: */*
>
* Request completely sent off
< HTTP/1.1 200 OK
< Connection: close
< Content-Length: 214
< Content-Type: application/json; charset=utf-8
< Server: castmedia 0.1.0
< Date: Sat,  6 Jul 2024 15:25:54 GMT
< Cache-Control: no-cache, no-store
< Expires: Mon, 26 Jul 1997 05:00:00 GMT
< Pragma: no-cache
< Access-Control-Allow-Origin: *
<
* Closing connection
{
  "mounts": [
    "/stream"
  ],
  "properties": {
    "admin": "admin@localhost",
    "description": "Internet radio!",
    "id": "castmedia 0.1.0",
    "location": "1.064646"
  },
  "stats": {
    "active_listeners": 0,
    "peak_listeners": 0,
    "start_time": 1720272275
  }
}
```

- `/api/mountinfo`: Get info of a specific mount

```
curl -v "http://127.0.0.1:9000/api/mountinfo?mount=/stream"
*   Trying 127.0.0.1:9000...
* Connected to 127.0.0.1 (127.0.0.1) port 9000
> GET /api/mountinfo?mount=/stream HTTP/1.1
> Host: 127.0.0.1:9000
> User-Agent: curl/8.7.1
> Accept: */*
>
* Request completely sent off
< HTTP/1.1 200 OK
< Connection: close
< Content-Length: 262
< Content-Type: application/json; charset=utf-8
< Server: castmedia 0.1.0
< Date: Sat,  6 Jul 2024 15:26:50 GMT
< Cache-Control: no-cache, no-store
< Expires: Mon, 26 Jul 1997 05:00:00 GMT
< Pragma: no-cache
< Access-Control-Allow-Origin: *
<
* Closing connection
{
  "metadata": {
    "title": "",
    "url": ""
  },
  "properties": {
    "bitrate": null,
    "content_type": "audio/mpeg",
    "description": null,
    "genre": null,
    "name": null,
    "public": false,
    "uagent": "Lavf/60.16.100",
    "url": null
  },
  "stats": {
    "active_listeners": 0,
    "peak_listeners": 0,
    "start_time": 1720272321
  }
}
```
