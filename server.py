#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author: wang_chao03
@project: pyraft
@file: server
@time: 2023/2/28
"""

import asyncio

from pyraft.run import start, stop
from pyraft.state import State


async def main(index, loop=None):
    await start(
        servers='10.201.38.14:8091,10.201.38.14:8092,10.201.38.14:8093', current_server_index=index or 0,
        loop=loop
    )
    while True:
        await asyncio.sleep(5, loop=loop)

        if (index == 0 and State.get_leader() == State.get_server_id('10.201.38.14', 8091)) \
                or (index == 1 and State.get_leader() == State.get_server_id('10.201.38.14', 8092)) \
                or (index == 2 and State.get_leader() == State.get_server_id('10.201.38.14', 8093)):
            await State.set_value('test', '123')


if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('-i', '--index', help="Current server's index", type=int, default=0)
    args = parser.parse_args()
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main(args.index, loop=loop))
    except KeyboardInterrupt:
        stop()
