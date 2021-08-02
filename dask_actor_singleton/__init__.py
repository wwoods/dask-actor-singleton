
import asyncio
import dask.distributed
import time

def discard(name, client=None):
    """Removes, if it exists, any existing dask_singleton_actor with the
    specified name. However, the variable is NOT guaranteed to be deleted before
    an immediately following `get` call.

    For this reason, various ttl features are provided for `get`.

    Named "discard" to fit `set` semantics.
    """

    if client is None:
        client = dask.distributed.get_client()

    # Weird edge case in dask code: https://distributed.dask.org/en/latest/_modules/distributed/variable.html
    assert client.status == 'running', "Dask client not running; will not delete correctly"

    var = dask.distributed.Variable(name=name, client=client)
    var.delete()


def get(name, create, *, client=None, ttl_create=0, ttl_get=0, priority=0):
    """
    Coordinates the allocation of a singleton across many workers.

    Returns an allocated [Actor](https://distributed.dask.org/en/latest/actors.html)
    proxy.

    Args:
        name: The exact name of the dask.distributed.Variable to be used.
        create: The lambda to be called, which returns an instance of the Actor
                class. Must consistently return the same result, regardless of
                where it is called. This is for singletons, after all.
        client: Optional; exact dask client.
        ttl_create: If positive, a new instance will allocated if the existing
                instance is more than this many seconds old.
        ttl_get: If positive, a new instance will be allocated if the existing
                instance hasn't been returned by a `get` call for more than
                this many seconds.

    By default, if a new actor must be created, it is done with priority 1e6.

    To change that priority, either use `dask.annotate` or pass the `priority`
    argument:

    ```python
    with dask.annotate(priority=100):
        dask_actor_singleton.get(...)
    ```

    A falsey value (like 0) will change to the default of 1e6.
    """

    if client is None:
        client = dask.distributed.get_client()

    var = dask.distributed.Variable(name=name, client=client)
    ractor = None

    if not priority:
        try:
            priority = dask.config.get('annotations.priority')
        except KeyError:
            pass
    if not priority:
        priority = 1e6

    def _cached_ractor_get():
        '''Ensures that an ractor is returned only if it abides by TTL
        conditions AND didn't raise an error in __init__.'''
        ractor = None
        actor = _try_get_actor(var)
        if actor is not None:
            future = actor.cache_check(ttl_create, ttl_get).result()
            if future is not None:
                try:
                    ractor = future.result()
                except:
                    pass
        return ractor

    ractor = _cached_ractor_get()
    if ractor is None:
        # Need to allocate, take a lock -- note that Semaphore is preferred
        # to Lock due to auto-lease expiration for lost workers:
        # See https://github.com/dask/distributed/issues/2362
        lock = dask.distributed.Semaphore(name=name+'__singleton_lock',
                scheduler_rpc=client.scheduler, loop=client.loop)
        with lock:
            # See if it was set between then and now
            ractor = _cached_ractor_get()
            if ractor is None:
                # Create, have lock and no existing, good Actor
                #future = client.submit(create, actor=True)
                future = client.submit(_ActorShell, create, actor=True,
                        priority=priority)
                # Allow this exception to trickle up __init__ errors
                actor = future.result()

                # Ensure that the inner actor was allocated successfully.
                # Will raise error if it was not.
                # Note: dask actor attribute access is implicit and blocking...
                # so `.future` is a round trip
                ractor = actor.future.result()

                # Finally, assign on success
                var.set(future)

    return ractor


def _try_get_actor(var):
    """Try to get the actor from a future. Return `None` on failure, instance
    otherwise.
    """
    future = None
    try:
        # Timeout is on scheduler node, so a low value is OK even with terrible
        # latency.
        future = var.get(timeout=1e-2)
    except asyncio.exceptions.TimeoutError:
        # Not yet created
        pass
    except Exception as e:
        # Any other error retrieving the variable -- not the result of the
        # future -- is probably a genuine error
        raise

    # This error shows up sometimes at future.result(); the current workflow
    # will see it and re-allocate the singleton.

    # if 'Worker holding Actor was lost' not in e:
    #     # Some other dask error worth re-raising
    #     raise

    actor = None
    if future is not None:
        try:
            actor = future.result()
        except:
            # Last allocation failed for some reason; try to replace
            pass

    return actor


class _ActorShell:
    """Proxy object which tracks TTL information.

    Due to the way Dask works, we'll create a type as a dynamic mixin...
    """
    future = None

    def __init__(self, create_fn):
        client = dask.distributed.get_client()
        self.future = client.submit(create_fn, actor=True, priority=1e6)

        self.singleton_time_create = time.monotonic()
        self.singleton_time_get = self.singleton_time_create


    def cache_check(self, ttl_create, ttl_get):
        """Check cache; if expired, return None. Else, returns the Future for
        this actor.
        """
        now = time.monotonic()
        if ttl_create > 0:
            if now - self.singleton_time_create >= ttl_create:
                return 1
        if ttl_get > 0:
            if now - self.singleton_time_get >= ttl_get:
                return 1

        # OK, update estimates
        self.singleton_time_get = now
        return self.future

