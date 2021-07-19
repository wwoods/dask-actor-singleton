
import asyncio
import dask.distributed

def get(name, create, client=None):
    """
    Coordinates the allocation of a singleton across many workers.

    Returns an allocated [Actor](https://distributed.dask.org/en/latest/actors.html)
    proxy.
    """

    if client is None:
        client = dask.distributed.get_client()

    future = None
    var = dask.distributed.Variable(name=name, client=client)
    try:
        # Timeout is on scheduler node, so a low value is OK even with terrible
        # latency.
        future = var.get(timeout=1)
    except asyncio.exceptions.TimeoutError:
        # Not yet created
        pass
    except Exception as e:
        # Check for transient failures
        if 'Worker holding Actor was lost' not in e:
            raise

    actor = None
    try:
        actor = future.result()
    except:
        # Last allocation failed for some reason; try to replace
        pass

    if actor is None:
        # Need to allocate, take a lock -- note that Semaphore is preferred
        # to Lock due to auto-lease expiration for lost workers:
        # See https://github.com/dask/distributed/issues/2362
        lock = dask.distributed.Semaphore(name=name+'__singleton_lock',
                scheduler_rpc=client.scheduler, loop=client.loop)
        with lock:
            try:
                # See if it was set between then and now
                future = var.get(timeout=1)
            except asyncio.exceptions.TimeoutError:
                pass
            else:
                # See if last allocation failed for any reason
                try:
                    actor = future.result()
                except:
                    pass

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


