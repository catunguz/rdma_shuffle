import os
from distexprunner import *

# TODO: You may need to change the IPs
IPS = ['192.168.122.136', '192.168.122.89']

DEBUG = True
CLEAN_BUILD = True  # Clean-build is required *once* after you change DEBUG !

server_list = ServerList(
    Server(f'devvm{i}', ip, 20000 + os.getuid(), ib_ip=ip) for i, ip in enumerate(IPS)
)


################################################################################
# Prepare & Compile
################################################################################

@reg_exp(servers=server_list, run_always=True)
def compile(servers):
    if CLEAN_BUILD:
        run_on_all(servers, f'rm -rf build/')

        if DEBUG:
            cmd = f'meson --native-file=native_cluster.ini -Db_sanitize=address build'
        else:
            cmd = f'meson --native-file=native_cluster.ini --buildtype=release -Db_lto=true build'
        run_on_all(servers, cmd)

    run_on_all(servers, 'ninja -C build/')


def fmt_args(args):
    from collections.abc import Iterable
    d = []
    for k, v in args.items():
        d.append(f'--{k}')
        if isinstance(v, Iterable) and not isinstance(v, str):
            d.append(','.join(v))
        else:
            d.append(f'{v}')
    return ' '.join(d)


################################################################################
# Basic Tests
################################################################################

parameter_grid = ParameterGrid(
    num_partitions=range(1, 9),
    num_rows=[1000, 100_000],

)


@reg_exp(servers=server_list, params=parameter_grid)
def basic_test(servers, num_partitions, num_rows):
    log('Basic test')

    args = {
        'node_ips': ','.join(s.ib_ip for s in servers),
        'mem_size': 1024*1024*10,
        'num_nodes': len(servers),
        'num_partitions': num_partitions,
        'num_rows': num_rows,
        'rdma_port': 30000 + os.getuid(),
    }

    env = {}
    if DEBUG:
        env['LD_PRELOAD'] = '/usr/lib/gcc/x86_64-linux-gnu/11/libasan.so'

    procs = ProcessGroup()
    for i, s in enumerate(servers):
        args['my_id'] = i
        cmd = f'./build/basic_1 {fmt_args(args)}'
        procs.add(s.run_cmd(cmd, stdout=Console(), env=env))
    procs.wait()


@reg_exp(servers=server_list, params=parameter_grid)
def basic_test2(servers, num_partitions, num_rows):
    log('Basic test')

    args = {
        'node_ips': ','.join(s.ib_ip for s in servers),
        'mem_size': 1024*1024*10,
        'num_nodes': len(servers),
        'num_partitions': num_partitions,
        'num_rows': num_rows,
        'rdma_port': 30000 + os.getuid(),
    }

    env = {}
    if DEBUG:
        env['LD_PRELOAD'] = '/usr/lib/gcc/x86_64-linux-gnu/11/libasan.so'

    procs = ProcessGroup()
    for i, s in enumerate(servers):
        args['my_id'] = i
        cmd = f'./build/basic_2 {fmt_args(args)}'
        procs.add(s.run_cmd(cmd, stdout=Console(), env=env))
    procs.wait()


@reg_exp(servers=server_list, params=parameter_grid)
def basic_test3(servers, num_partitions, num_rows):
    log('Basic test')

    args = {
        'node_ips': ','.join(s.ib_ip for s in servers),
        'mem_size': 1024*1024*10,
        'num_nodes': len(servers),
        'num_partitions': num_partitions,
        'num_rows': num_rows,
        'rdma_port': 30000 + os.getuid(),
    }

    env = {}
    if DEBUG:
        env['LD_PRELOAD'] = '/usr/lib/gcc/x86_64-linux-gnu/11/libasan.so'

    procs = ProcessGroup()
    for i, s in enumerate(servers):
        args['my_id'] = i
        cmd = f'./build/basic_3 {fmt_args(args)}'
        procs.add(s.run_cmd(cmd, stdout=Console(), env=env))
    procs.wait()



