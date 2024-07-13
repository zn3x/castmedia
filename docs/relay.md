
# Relaying

Process in which a server mirrors one more streams from another server, castmedia follows the master/slave model similar to icecast. This is done mainly to offload traffic from a master server to slave servers.

There are two types of relaying:

- Transparent: where the master server does not know it is being relayed, no extra configuration is needed on the master. Slave server may run to limits or other potential errors as it is seen as a normal listener by the master server. This mode is compatible with icecast.

- Authenticated: In this case the master is aware of the slave server. Both the master and the server need to be configured. This mode is exclusive to castmedia.

## Transparent

To relay sources in a master server, we add a `master` section to the configuration file of the slave server as the following:

```yaml
master:
  - url: master_url # scheme can either be http or https
    relay_scheme:
      type: transparent
      update_interval: 10000 # slave will keep polling every given x milliseconds to mount new sources from master
```

## Authenticated

In this case both the master and slave servers need extra configuration.

In the master server configuration file we add a new user with role slave to the user section:

```yaml
account:
  slave:
    role: slave
    pass: 0$pass
```

And in the slave server configuration file in master section:

```yaml
master:
  - url: master_url
    relay_scheme:
      type: authenticated
      user: slave
      pass: pass
      reconnect_timeout: 5000 # when establishing connection to master fails, this is the timeout period in ms
      stream_on_demand: true
```

The `stream_on_demand` is a flag used to enable `on_demand` feature for reducing bandwidth consumption, slave will only poll source stream when at least one listener is connected.
