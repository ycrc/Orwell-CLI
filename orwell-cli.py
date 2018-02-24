#!/usr/bin/env python
from __future__ import print_function
import re
import argparse
import subprocess
from collections import defaultdict as dd
from bisect import bisect_left

desc = """https://github.com/ycrc/Orwell-CLI

A utility to view summary slurm node status and usage.

"""


show_cpu = True 
blocks = {}
blocks['not a node'] = ' '
blocks['idle'] = '_'
blocks['down'] = 'X'
blocks['reserved'] = 'r'
usage_chars = [u'\u2581', u'\u2582', u'\u2583', 
               u'\u2584', u'\u2585', u'\u2586', u'\u2587']
usage_chars = [x.encode('utf-8') for x in usage_chars]
usage_values = [1/8.0, 1/4.0, 3/8.0, 1/2.0, 5/8.0, 3/4.0, 7/8.0]
blocks_usage = dict(zip(usage_values, usage_chars))

sinfo_cmd = ['sinfo', '--format=%all', '-a']
node_regex = re.compile('([a-z]+)(\d\d)*n?(\d\d*)')

node_info = dd(lambda: (blocks['not a node'], ''))
chassis_set = set()
node_num_maxes = dd(lambda: 1)

def get_pad(list_of_things):
    return max(map(len, list_of_things))+2

def print_legend():
    print("Legend")
    states = sorted(blocks.keys())
    pad = get_pad(states+['node usage'])
    for state in blocks.keys():
        state_out = (state+': ').ljust(pad)
        print('{}|{}|'.format(state_out, blocks[state]))
    nu = 'node usage: '.ljust(pad)
    usage_legend = '|'.join(usage_chars)
    print('{}|{}|'.format(nu, usage_legend))
    print('{}^1%{}100%^\n'.format(' ' * len(nu), ' ' * (len(usage_values)*2-7)))
    

def get_closest(nums, my_num):
    """
    If two numbers are equally close, return the smaller number.
    """
    pos = bisect_left(nums, my_num)
    if pos == 0:
        return nums[0]
    if pos == len(nums):
        return nums[-1]
    before = nums[pos - 1]
    after = nums[pos]
    if after - my_num < my_num - before:
       return after
    else:
       return before

def get_subprocess_lines(cmd):
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    for line in p.stdout:
        yield line.strip()
    p.wait()

def get_usage_block(state, usage):
    if state.startswith('mix') or state.startswith('alloc'):
        return blocks_usage[get_closest(usage_values, usage)]
    if state.startswith('idle'):
        return blocks['idle']
    if state.startswith('reserv'):
        return blocks['reserved']
    else:
        return blocks['down']

def split_node(node_name):
    n_match = node_regex.match(node_name)
    groups = n_match.groups()
    # if node name is like c13n05
    if groups[1] is not None:
        chassis = groups[0]+groups[1]
    # if node name is like gpu02 or bigmem05
    else:
        chassis = groups[0]
    node_num = int(groups[2])
    return chassis, node_num

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=desc, prog='orwell-cli', 
                                     formatter_class=argparse.RawTextHelpFormatter)
    
    parser.add_argument('--mem',
                        action='store_false',
                        help='Show allocated memory instead of CPU.')
    args = parser.parse_args()
    show_cpu = args.mem
    for line in get_subprocess_lines(sinfo_cmd):
        if line.startswith('AVAIL'):
            header = re.split(' ?\|', line)
        else:
            sinfo = dict(zip(header,re.split(' ?\|', line)))
            in_use,idle,unavailable,cores = tuple(map(int, sinfo['CPUS(A/I/O/T)'].split('/')))
            used_cores = in_use / float(cores)
            if show_cpu is True:
                usage_block = get_usage_block(sinfo['STATE'], used_cores)
            else:
                if sinfo['FREE_MEM'] == 'N/A':
                    used_memory = 0
                else:
                    free_mem = float(sinfo['FREE_MEM'])
                    total_mem = float(sinfo['MEMORY'])
                    used_memory = (total_mem - free_mem) / total_mem
                usage_block = get_usage_block(sinfo['STATE'], used_memory)
            chassis, node_num = split_node(sinfo['HOSTNAMES'])
            chassis_set.add(chassis)
            if node_num_maxes[chassis] < node_num:
                    node_num_maxes[chassis] = node_num
            node_info['{}{:02d}'.format(chassis, node_num)]=(usage_block, '')
    print_legend()
    chassis_pad = get_pad(chassis_set)
    for chassis in sorted(chassis_set):
        print((chassis+': ').ljust(chassis_pad), end='')
        line = []
        for n in range(1,node_num_maxes[chassis]+1):
            line.append(node_info['{}{:02d}'.format(chassis, n)][0])
        print('|{}|'.format('|'.join(line)))
