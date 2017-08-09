
redis-keys-stat
===============

This tool scan Redis keys and prints data structure and stat.

## Install

```bash
pip install -r requirements.txt
```

## Motivation

Easy way to get idea about:
 - how Redis data layout looks like;
 - how many keys in each section.


## Usage

```bash
$ ./redis-keys-stat redis://redis.host:port/dbNum

example

$ ./redis-keys-stat redis://localhost:6380/0
```


### Result

```
Redis: redis://pp.local:6380/0
Start scanning all keys
 > Found superkey: "stat:"
 > Found superkey: "stat:failed:"
 ...
 > Sorting keys ...
Result
limit_fetch: (11)
limit_fetch:limit: (11)
session: (8)
stat: (48)
stat:failed: (23)
stat:processed: (23)
vendor_ids: (2)
```

Looking at result it obvious that we have structure

```
stat:
  failed:*
  processed:*
```

For big datasets it takes some time to scan all keys.
