#!/usr/bin/env python
from __future__ import print_function
import re
import argparse
from textwrap import wrap
from subprocess import check_output
from collections import defaultdict as dd
from bisect import bisect_left

parts = set([x.split()[0] for x in check_output(['sinfo', '-h'], universal_newlines=True).split('\n')[:-1]])
desc = """https://github.com/ycrc/Orwell-CLI
A utility to view slurm node status and usage.

Partitions found (* means default):
{}
""".format('\n'.join(wrap(', '.join(sorted(parts)), 80)))

colors = {'black': '30', 'blue': '34', 'cyan': '36', 'green': '32', 
          'magenta': '35', 'red': '31', 'white': '37', 'yellow': '33'}
hi_color = 'red'

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

node_info = dd(lambda: {'block':blocks['not a node'], 'partition':set(), 
                        'features':set(), 'users':set(), 'jobs':set(), 
                        'highlight':False})
chassis_set = set()
node_num_maxes = dd(lambda: 1)

def get_pad(list_of_things):
    return max(map(len, list_of_things))+2

def highlight(text):
    return '\033[{}m{}\033[0m'.format(colors[hi_color], text)

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

def update_node_info(sinfo):
    
    in_use,idle,unavailable,cores = tuple(map(int, sinfo['CPUS(A/I/O/T)'].split('/')))
    cpu_usage = in_use / float(cores)
    if sinfo['FREE_MEM'] == 'N/A':
        mem_usage = 0
    else:
        free_mem = float(sinfo['FREE_MEM'])
        total_mem = float(sinfo['MEMORY'])
        mem_usage = (total_mem - free_mem) / total_mem

    if show_usage == 'cpu':
        usage_block = get_usage_block(sinfo['STATE'], cpu_usage)
    elif show_usage == 'ram':
        usage_block = get_usage_block(sinfo['STATE'], mem_usage)
    elif show_usage == 'both':
        usage_block = (get_usage_block(sinfo['STATE'], cpu_usage) + 
                       get_usage_block(sinfo['STATE'], mem_usage))
           
    chassis, node_num = split_node(sinfo['HOSTNAMES'])
    chassis_set.add(chassis)
    node_name = '{}{:02d}'.format(chassis, node_num)

    if node_num_maxes[chassis] < node_num:
            node_num_maxes[chassis] = node_num
    node_info[node_name]['block'] = usage_block
    node_info[node_name]['partition'].add(sinfo['PARTITION'])
    [node_info[node_name]['features'].add(f) for f in sinfo['AVAIL_FEATURES'].split(',')]
    return chassis, node_num

def filter_node(node_name, filter_tag, query):
    if node_info[node_name]['highlight'] is False:
        for sub_query in query.split(','):
            if sub_query.lower() in node_info[node_name][filter_tag]:
                node_info[node_name]['highlight'] = True

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=desc, prog='orwell-cli', 
                                     formatter_class=argparse.RawTextHelpFormatter)
    
    parser.add_argument('-s', '--show',
                        default='cpu',
                        metavar='cpu|ram|both',
                        choices=['cpu', 'ram', 'both'],
                        help='Show proportion of allocated CPUs, RAM, or both. Order is CPU then RAM for both.')
    parser.add_argument('-l', '--legend',
                        action='store_true',
                        help='Show legend.')
    parser.add_argument('-c', '--color',
                        metavar='color',
                        help='Color to use for highlighting. Default: {}\n  Options: {}'.format(hi_color, ', '.join(colors.keys())))
    parser.add_argument('-p', '--partition',
                        metavar='partition(s)',
                        help='Highlight nodes that are members of the given partition(s), comma separated')
    parser.add_argument('-f', '--feature',
                        metavar='feature(s)',
                        help='Highlight nodes with the given feature(s), comma separated')
#    parser.add_argument('-j', '--job',
#                        metavar='jobid(s)',
#                        help='Highlight nodes where jobs with jobid(s) are running, comma separated')
#    parser.add_argument('-u', '--user',
#                        metavar='users(s)',
#                        help='Highlight nodes where the given user(s) are running jobs, comma separated')

    args = parser.parse_args()
    show_usage = args.show
    if args.legend:
        print_legend()
    if args.color is not None:
        if args.color.lower() in colors.keys():
            hi_color = args.color.lower()
        else:
            print('Unrecognized color "{}", using default.'.format(args.color))
    node_filters = []
    if args.partition is not None:
        node_filters.append(('partition', args.partition))
    if args.feature is not None:
        node_filters.append(('features', args.feature))

    chassis_set = set()
    raw_sinfo = check_output(sinfo_cmd, universal_newlines=True).split('\n')
    header = re.split(' ?\|', raw_sinfo.pop(0))
    for line in raw_sinfo[:-1]:
        sinfo = dict(zip(header,re.split(' ?\|', line)))
        chassis, node_number = update_node_info(sinfo)
        chassis_set.add(chassis)
    chassis_pad = get_pad(chassis_set)
    
    for chassis in sorted(chassis_set):
        print((chassis+': ').ljust(chassis_pad), end='')
        line = []
        for n in range(1,node_num_maxes[chassis]+1):
            node = '{}{:02d}'.format(chassis, n)
            if len(node_filters) != 0:
                for filt in node_filters:
                    filter_node(node, *filt)
            if node_info[node]['highlight']:
                line.append(highlight(node_info[node]['block']))
            else:
                line.append(node_info[node]['block'])
        print('|{}|'.format('|'.join(line)))
