#!/usr/bin/env python
from __future__ import print_function
import os
import re
import string
import argparse
import subprocess
from os import path
from textwrap import wrap
from collections import defaultdict as dd
from itertools import chain, cycle
from bisect import bisect_left

## Constants
colors = {'black': '30', 'blue': '34', 'cyan': '36', 'green': '32', 
          'magenta': '35', 'red': '31', 'white': '37', 'yellow': '33'}
hi_color = 'red'
nelson = False
job_glyphs = cycle(string.letters+string.digits)

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

slurm_prefix = '/etc/slurm'
sinfo_cmd = ['sinfo', '--format=%all', '-a']
sacct_cmd = ['sacct', '-XaPsR', '-oJobID,JobName,User,Account,NodeList']
node_regex = re.compile('([a-z]+)(\d\d)*n?(\d\d*)')
gpu_regex = re.compile('NodeName=([a-zA-Z\d\[\],\-]+).+Type=([\w\d]+)\W+.*')

## Globals
node_info = dd(lambda: {'block':blocks['not a node'], 'partition':set(), 
                        'feature':set(), 'user':set(), 'job':set(), 
                        'account':set(), 'gpu':set()})

job_map = {}
chassis_set = set()
node_num_maxes = dd(lambda: 1)

## Functions
def get_pad(list_of_things):
    return max(map(len, list_of_things))+2

def highlight_node(text):
    return '\033[{}m{}\033[0m'.format(colors[hi_color], text)

def _wrap(s, n):
    return '\n'.join(wrap(s, n))

def get_subprocess_lines(cmd):
    pipe = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    for line in pipe.stdout:
        yield line.strip()
    pipe.wait()

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
    node_match = node_regex.match(node_name)
    groups = node_match.groups()
    # if node name is like c13n05
    if groups[1] is not None:
        chassis = groups[0]+groups[1]
    # if node name is like gpu02 or bigmem05
    else:
        chassis = groups[0]
    node_num = int(groups[2])
    return chassis, node_num

def expand_hostlist(hostlist):
        return chain.from_iterable(_expand_hostlist(hostlist))
def _expand_hostlist(hostlist):
    in_bracket = p_beg = p_end = 0
    for i, c in enumerate(hostlist):
        if not in_bracket and c == ",":
            yield _expand_part(hostlist[p_beg:p_end])
            p_beg, p_end = i+1, i
        p_end += 1
        in_bracket += int(c == "[") + -1*int(c == "]")
    yield _expand_part(hostlist[p_beg:p_end])
def _expand_part(p):
    if "[" in p:
        r_beg, r_end, prefix = p.index("["), p.index("]"), p[:p.index("[")]
        for sub_r in p[r_beg+1:r_end].split(","):
            if "-" not in sub_r:
                yield prefix + sub_r
            else:
                lo,hi = sub_r.split("-", 1)
                for i in range(int(lo), int(hi)+1):
                    yield prefix + str(i).zfill(len(lo))
    else:
        yield p

def get_gpus():
    nodes = []
    gpus = []
    with open(path.join(slurm_prefix, 'gres.conf')) as gres:
        for line in gres:
            gpu_match = gpu_regex.match(line)
            if gpu_match is not None:
                groups = gpu_match.groups()
                if groups is not None and groups[0] is not None and groups[1] is not None:
                    hostlist, gpu = groups
                    for node in expand_hostlist(hostlist):
                        yield (node, gpu)

def get_help():
    parts = set([x.split()[0] for x in get_subprocess_lines(['sinfo', '-h'])])
    gpu_info = get_gpus()
    if gpu_info is None:
        gpus = 'None'
    else:
        gpus = set([x[1] for x in gpu_info])

    # feats = get_features()
    return ('https://github.com/ycrc/Orwell-CLI\n'+
            'A utility to view slurm node status and usage.\n\n'+
            'Partitions found (* means default):\n'+
            _wrap(', '.join(sorted(parts)), 80)+
            '\n\n'+
            'GPUs found:\n'+
            _wrap(', '.join(sorted(gpus)), 80)+
            '\n'
            )

def add_gpu_info():
    gpu_info = get_gpus()
    if gpu_info is None:
        pass
    else:
        for (nodelist, gpu) in gpu_info:
            for node in expand_hostlist(nodelist):
                node_info['{}{:02d}'.format(*split_node(node))]['gpu'].add(gpu)

def update_node_info(sinfo):
    in_use,idle,unavailable,cores = tuple(map(int, sinfo['CPUS(A/I/O/T)'].split('/')))
    cpu_usage = in_use / float(cores)
    if sinfo['FREE_MEM'] == 'N/A':
        mem_usage = 0
    else:
        free_mem = float(sinfo['FREE_MEM'])
        total_mem = float(sinfo['MEMORY'])
        mem_usage = (total_mem - free_mem) / total_mem

    chassis, node_num = split_node(sinfo['HOSTNAMES'])
    chassis_set.add(chassis)
    node_name = '{}{:02d}'.format(chassis, node_num)

    if show_usage == 'cpu':
        node_info[node_name]['block'] = get_usage_block(sinfo['STATE'], cpu_usage)
    elif show_usage == 'ram':
        node_info[node_name]['block'] = get_usage_block(sinfo['STATE'], mem_usage)
    elif show_usage == 'both':
        node_info[node_name]['block'] = (get_usage_block(sinfo['STATE'], cpu_usage) + 
                                         get_usage_block(sinfo['STATE'], mem_usage))
 
    if node_num_maxes[chassis] < node_num:
            node_num_maxes[chassis] = node_num
    node_info[node_name]['partition'].add(sinfo['PARTITION'])
    [node_info[node_name]['feature'].add(f) for f in sinfo['AVAIL_FEATURES'].split(',')]
    chassis_set.add(chassis)

def update_job_info(sacct):
    for node in expand_hostlist(sacct['NodeList']):
        chassis, node_num = split_node(node)
        node_name = '{}{:02d}'.format(chassis, node_num)
        node_info[node_name]['job'].add(sacct['JobID']) 
        # also add array jobid
        base_jobid = sacct['JobID'].split('_')[0]
        if show_usage == 'job':
            if base_jobid not in job_map:
                job_map[base_jobid] = next(job_glyphs)
            node_info[node_name]['block'] = job_map[base_jobid]
        node_info[node_name]['job'].add(base_jobid)
        node_info[node_name]['user'].add(sacct['User'])
        node_info[node_name]['account'].add(sacct['Account'])

def filter_node(node_name, highlight_mode, filter_tag, query):
    bools = []
    highlight = False
    for sub_query in query.split(','):
        if sub_query.lower() in node_info[node_name][filter_tag]:
            bools.append(True)
        else:
            bools.append(False)
    if highlight_mode == 'or':
        return any(bools)
    if highlight_mode == 'and':
        return all(bools)

def parse_slurm(cmd, header_hint, update_func):
    for line in get_subprocess_lines(cmd):
        if line.startswith(header_hint):
            header = re.split(' ?\|', line)
        else:
            slrm = dict(zip(header,re.split(' ?\|', line)))
            update_func(slrm)

def print_node_layout(chassis_set, show_usage, node_filters, highlight_mode):
    chassis_pad = get_pad(chassis_set)
    for chassis in sorted(chassis_set):
        print((chassis+': ').ljust(chassis_pad), end='')
        line = []
        for n in range(1,node_num_maxes[chassis]+1):
            node = '{}{:02d}'.format(chassis, n)
            if show_usage == 'both' and node_info[node]['block'] == blocks['not a node']:
                node_info[node]['block'] += blocks['not a node']
            if len(node_filters) != 0:
                bools = []
                highlight = False
                for filt in node_filters:
                    bools.append(filter_node(node, highlight_mode, *filt))
                if highlight_mode == 'or':
                    highlight = any(bools)
                if highlight_mode == 'and':
                    highlight = all(bools)
                if highlight:
                    line.append(highlight_node(node_info[node]['block']))
                else:
                    line.append(node_info[node]['block'])
            else:
                line.append(node_info[node]['block'])
        print('|{}|'.format('|'.join(line)))

## Main
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=get_help(), prog='orwell-cli', 
                                     formatter_class=argparse.RawTextHelpFormatter)
    
    parser.add_argument('-s', '--show',
                        default='cpu',
                        metavar='cpu|ram|both',
                        choices=['cpu', 'ram', 'both', 'job'],
                        help=_wrap(('Show proportion of allocated CPUs, RAM, both, or job layout. '+
                              'Order when displaying proportion of both is CPU, RAM. '+
                              'Showing "job" will assign a letter or number to each job and display the '+
                              'last job running on each node. Makes the most sense on clusers '+
                              'with exclusive node allocation.'), 70))
    parser.add_argument('-l', '--legend',
                        action='store_true',
                        help='Show legend.')
    parser.add_argument('-c', '--color',
                        metavar='color',
                        help=('Color to use for highlighting. Default: ' +
                              '{}\n  Options: {}'.format(hi_color, ', '.join(colors.keys()))))
    parser.add_argument('-b', '--bool',
                        default='or',
                        choices=['and','or'],
                        help=_wrap(('Logic to use when combining filters. "or" will highlight a node '+
                            'if any of the filters match, "and" will only highlight a node if '+
                            'all filters match. Default is "or".'), 70))
    parser.add_argument('-p', '--partition',
                        metavar='partition',
                        action='append',
                        help='Highlight nodes that are members of the given partition(s), comma separated')
    parser.add_argument('-f', '--feature',
                        metavar='feature',
                        action='append',
                        help='Highlight nodes with the given feature(s), comma separated')
    parser.add_argument('-g', '--gpu',
                        metavar='gpu_type',
                        action='append',
                        help='Highlight nodes with the given gpu(s) available, comma separated')
    parser.add_argument('-j', '--job',
                        metavar='jobid',
                        action='append',
                        help='Highlight nodes where jobs with jobid(s) are running, comma separated')
    parser.add_argument('-u', '--user',
                        metavar='user',
                        action='append',
                        help='Highlight nodes where the given user(s) are running jobs, comma separated')
    parser.add_argument('-A', '--account',
                        metavar='account',
                        action='append',
                        help='Highlight nodes where the given account(s) are running jobs, comma separated')

    args = vars(parser.parse_args())
    show_usage = args['show']
    if args['legend']:
        print_legend()
    if args['color'] is not None:
        if args.color.lower() in colors.keys():
            hi_color = args.color.lower()
        else:
            print('Unrecognized color "{}", using default.'.format(args.color))
    
    node_filters = []
    for filt in ['partition', 'feature']:
        if args[filt] is not None:
            for f in args[filt]:
                node_filters.append((filt, f))
    if args['gpu'] is not None:
        for g in args['gpu']:
            node_filters.append(('gpu', g))
        add_gpu_info()

    # get job/user info if asked
    get_job_info = False
    for filt in ['job', 'user', 'account']:
        if args[filt] is not None:
            for f in args[filt]:
                node_filters.append((filt, f)) 
            get_job_info = True
    if show_usage == 'job':
        get_job_info = True

    if get_job_info:
        parse_slurm(sacct_cmd, 'JobID|', update_job_info)

    # get node/partition info
    parse_slurm(sinfo_cmd, 'AVAIL|', update_node_info)
    
    # print node layout
    print_node_layout(chassis_set, show_usage, node_filters, args['bool'])

