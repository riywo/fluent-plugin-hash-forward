# fluent-plugin-hash-forward

Fluentd plugin to keep forwarding messages of a specific tag pattern to a specific node

## Configuration

Example:

    <match pattern>
      type hash_forward
      flush_interval 1s
    
      <server>
        host 192.168.1.3
        port 24224
      </server>
      <server>
        host 192.168.1.4
        port 24224
      </server>
    
      <secondary>
        type file
        path /var/log/fluent/forward-failed
      </secondary>
    </match>

## Parameters

Basically same with out\_forward plugin. See [http://docs.fluentd.org/articles/out_forward](http://docs.fluentd.org/ja/articles/out_forward). 

Following parameters are additionally available:


* keepalive (bool)

    Keepalive connection. Default is `false`.

* keepalive_time (time)

    Keepalive expired time. Default is nil (which means to keep connection as long as possible).

* heartbeat_type

    The transport protocol to use for heartbeats. The default is “udp”, but you can select “tcp” as well.
    Furthermore, in hash_forward, you can also select "none" to disable heartbeat. 

* hash\_key\_slice *min*..*max*

    Use sliced `tag` as a hash key to determine a forwarding node. Default: use entire `tag`. 

    For example, assume tags of input messages are like

        foo.bar.host1
        foo.bar.host2

    but, you want to forward these messages to the same node, configure like

        hash_key_slice 0..-2

    then, hash\_key becomes as `foo.bar` which results in forwarding these messages to the same node.

    FYI: This option behaves like `tag.split('.').slice(min..max)`.

## ToDo

* Consistent hashing

   * Consistent hashing is useful on adding or removing nodes dynamically, but currently `out_hash_forward` does not support such a dynamical feature, so consistent hashing is just useless now. To effectively support consistent hashing, this plugin must support ways to add or remove nodes dynamically by preparing http api or reading nodes information from redis or memcached, etc. 

## ChangeLog

See [CHANGELOG.md](CHANGELOG.md) for details. 

## Copyright

* Copyright
  * Copyright (c) 2012- Ryosuke IWANAGA (riywo)
  * Copyright (c) 2013- Naotoshi SEO (sonots)
* License
  * Apache License, Version 2.0
