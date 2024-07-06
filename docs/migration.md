
# Migration and zero-downtime

One of the key features of castmedia is that it lets you update your configuration file or upgrade castmedia without downtime.

Migration must first be enabled in the configuration file in the `migrate` section:

```yaml
migrate:
  # This must be enabled in both current instance and successor instance for migration to be successfully done
  enabled: true
  # The unix socket address where an instance waits for a successor instance to pass it's state to
  bind: /tmp/migrate.sock
```

An already running instance with migration disabled won't be migrated. If you run castmedia with migration disabled you will get a warning notice.

It is also worth noting that having listeners with TLS is not supported with migration.

To migrate to new instance, you simply run another castmedia instance with same `migrate.bind` unix socket file location. It can even be the same configuration of old instance with updates to it.


## Migration in relaying

TODO
