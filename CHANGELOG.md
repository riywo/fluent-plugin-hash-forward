# 0.3.5 (2014/03/24)

Fixes

* Explicitly close standby node socket on recovering in keepalive
* Explicitly close socket if an error raised even in keepalive

# 0.3.4 (2014/03/19)

Fixes

* Explicitly close socket at the case non-keepalive

# 0.3.3 (2014/03/17)

Enhancement:

* Support `heartbeat_type` `none`

# 0.3.2 (2014/02/04)

Enhancement:

* Support `log_level` option of Fleuntd v0.10.43

# 0.3.1 (2013/12/11)

Changes

* Change default `keepalive` option to false

# 0.3.0 (2013/12/11)

Enhancement

* Add `keepalive` and `keepalive_time` option

# 0.2.0 (2013/11/02)

Enhancement

* Handling weight

# 0.1.0 (2013/11/02)

Enhancement

* Cache nodes

# 0.0.3 (2013/11/01)

Changes

* No dependency on murmurhash3 any more

# 0.0.2 (2013/10/31)

Changes

* Change `hash_key` option to `hash_key_slice` option. Stopped to use a placeholder. 

# 0.0.1 (2013/10/31)

* Initial release

