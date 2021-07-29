
import asyncio
import dask.distributed

def discard(name, client=None):
    """Removes, if it exists, any existing dask_singleton_actor with the
    specified name.

    Called "discard" to fit `set` semantics.
    """

    if client is None:
        client = dask.distributed.get_client()

    # Weird edge case in dask code: https://distributed.dask.org/en/latest/_modules/distributed/variable.html
    assert client.status == 'running', "Dask client not running; will not delete correctly"

    var = dask.distributed.Variable(name=name, client=client)
    var.delete()


def get(name, create, client=None):
    """
    Coordinates the allocation of a singleton across many workers.

    Returns an allocated [Actor](https://distributed.dask.org/en/latest/actors.html)
    proxy.
    """

    if client is None:
        client = dask.distributed.get_client()

    var = dask.distributed.Variable(name=name, client=client)
    actor = _try_get_actor(var)

    if actor is None:
        # Need to allocate, take a lock -- note that Semaphore is preferred
        # to Lock due to auto-lease expiration for lost workers:
        # See https://github.com/dask/distributed/issues/2362
        lock = dask.distributed.Semaphore(name=name+'__singleton_lock',
                scheduler_rpc=client.scheduler, loop=client.loop)
        with lock:
            # See if it was set between then and now
            actor = _try_get_actor(var)

            if actor is None:
                # Create, have lock and no existing, good Actor
                future = client.submit(create, actor=True)
                # Allow this exception to trickle up __init__ errors
                actor = future.result()

                # Finally, assign on success
                var.set(future)
            else:
                # Retrieved successfully from previous init
                pass

    return actor


def _try_get_actor(var):
    """Try to get the actor from a variable. Return `None` on failure, instance
    otherwise.
    """
    future = None
    try:
        # Timeout is on scheduler node, so a low value is OK even with terrible
        # latency.
        future = var.get(timeout=1)
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


