
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

To migrate to new instance, you simply run another castmedia instance with same `migrate.bind` unix socket file location. It can even be the same configuration file of old instance with updates to it.


## Migration in relaying

Migration for relaying has multiple scenarios in which migration will cause downtime:

- Changing master configuration:
    - Slave user removed:
        - Slave server using authenticated mode with this user: Downtime
    - Listening address removed or changed (even just the port):
        - It's still used by slave server: Downtime
- Changing slave configuration:
    - Switching from transparent to authenticated mode:
        - Master server is not already configured with needed slave user: Downtime
    - Enabling or disabling `on_demand` feature: No downtime
    - Switching from authenticated to transparent mode: No downtime



## Caveats

- During migration, connections that do not respect new configuration constraints will be dropped even if they were allowed previously.
- TLS migration is not supported and will probably never be supported due to challenges that comes with passing TLS connection to another process.
