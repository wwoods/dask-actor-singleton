
import dask_actor_singleton

import dask.distributed
import time

class Actor:
    def beep(self):
        return 'boop'

def main():
    c = dask.distributed.Client()

    for _ in range(4):
        a = dask_actor_singleton.get('a', Actor, ttl_get=1e-3)
        time.sleep(0.5)

    input('Ensure localhost:8787 shows only 2 in-memory tasks across cluster; press enter to exit')


if __name__ == '__main__':
    main()

