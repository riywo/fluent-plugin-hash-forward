# fluent-plugin-hash-forward

## Component

### HashForwardOutput

Fluentd plugin to forward some servers using calculated hash of tag

- Forward some servers
- Same tag messages forward to the same server
    - Using Murmurhash

## Configuration

## HashForwardOutput

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

## TODO

* tests
* documents

## Copyright

* Copyright
  * Copyright (c) 2012- Ryosuke IWANAGA (riywo)
* License
  * Apache License, Version 2.0
