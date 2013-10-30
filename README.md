# fluent-plugin-hash-forward

Fluentd plugin to keep forwarding messages of a specific tag pattern to a specific node

- Forward some servers
- Same tag messages forward to the same server
    - Using Murmurhash

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

* remove\_prefix

    Specify a string to remove a prefix from the input tag

* add\_prefix

    Specify a string to add a prefix from the input tag

## Copyright

* Copyright
  * Copyright (c) 2012- Ryosuke IWANAGA (riywo)
  * Copyright (c) 2013- Naotoshi SEO (sonots)
* License
  * Apache License, Version 2.0
