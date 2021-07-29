
import dask.distributed
import dask_actor_singleton

class MyClass:
    def __init__(self, value):
        self.value = value
    def inc(self):
        self.value += 1
        return self.value


def test_basic():
    client = dask.distributed.Client()

    act = dask_actor_singleton.get('a', create=lambda: MyClass(1), client=client)
    assert act.inc().result() == 2
    assert act.inc().result() == 3
    act2 = dask_actor_singleton.get('a', create=lambda: MyClass(100),
            client=client)
    assert act2.inc().result() == 4
    assert act.inc().result() == 5
    act3 = dask_actor_singleton.get('b', create=lambda: MyClass(1), client=client)
    assert act3.inc().result() == 2
    assert act2.inc().result() == 6

    # Test deletion
    dask_actor_singleton.discard('a')
    # Old references still work
    assert act.inc().result() == 7
    # But the name will not be found
    act_new = dask_actor_singleton.get('a', create=lambda: MyClass(-10),
            client=client)
    assert act_new.inc().result() == -9

    # Test losing an actor by killing all workers
    def raiser():
        raise SystemExit
    from pprint import pprint
    pprint(client.scheduler_info())
    try:
        client.run(raiser)
    except:
        pass
    pprint(client.scheduler_info())

    var = dask.distributed.Variable(name='b', client=client)
    fut = var.get(timeout=1)  # Should be fine
    try:
        fut.result()
    except Exception as e:
        assert 'Worker holding Actor was lost' in str(e)
    else:
        assert False and 'No exception raised?'

    act_lost = dask_actor_singleton.get('b', create=lambda: MyClass(-100),
            client=client)
    assert act_lost.inc().result() == -99

