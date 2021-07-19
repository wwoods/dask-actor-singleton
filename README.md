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
```

History
-------
* 2021-07-19 v1.0 release.
