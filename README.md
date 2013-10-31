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

* hash\_key

    Specify a placeholder string to be used as a key for hashing. See Placeholders section for more details. Default uses `${tag}`as a hash key.

### Placeholders

You can use following placeholders:

* ${tag} input tag
* ${tags} input tag splitted by '.'

It is also possible to write a ruby code in placeholders, so you may write some codes as

* ${tags[0]}
* ${tags.last}

For example, if your messages have tags like

    foo.host1
    foo.host2

but, you want to send `foo.*` to the same node, 

    hash_key ${tags[0..-2]}

should work well. 

## Copyright

* Copyright
  * Copyright (c) 2012- Ryosuke IWANAGA (riywo)
  * Copyright (c) 2013- Naotoshi SEO (sonots)
* License
  * Apache License, Version 2.0
