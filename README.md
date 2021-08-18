dask-actor-singleton
====================

This package works around common transient errors and pitfalls in creating a singleton instance of an [Actor](https://distributed.dask.org/en/latest/actors.html) in [Dask](https://docs.dask.org/en/latest/). It provides a clean interface for retrieving the singleton instance, and allocating it when necessary.

Usage
-----

```python
import dask_actor_singleton

class MyActor:
    def __init__(self, arg):
        self.value = arg
    def inc(self):
        self.value += 1
        return self.value

client = dask.distributed.Client()
actor = dask_actor_singleton.get('my_actor', create=lambda: MyActor(8))
print(actor.inc().result())  # 9
# Now, on a different computer / dask.distributed.Client, run this script again:
# ...
print(actor.inc().result())  # 10
# If ever the singleton should be deleted, one may call:
dask_actor_singleton.discard('my_actor')
```

History
-------
* 2021-08-18 v1.3.1 release. Free old actor when re-allocating to potentially reduce memory load.
* 2021-08-02 v1.3.0 release. Priority argument added to improve responsiveness alongside larger processing loads.
* 2021-07-29 v1.2.0 release. Added TTL support for `get`, and better documentation for `discard`.
* 2021-07-29 v1.1.1 release. Fixed issue with "Working holding Actor was lost"
* 2021-07-29 v1.1 release. Supports `discard` to purge a cached singleton.
* 2021-07-19 v1.0 release.

