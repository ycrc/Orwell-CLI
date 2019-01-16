#!/usr/bin/env python
import sys
import argparse
import subprocess
from collections import defaultdict as dd

size_multipliers = {'M':1, 'G':1024, 'T':1024**2}
core_node_keys = {'c':'ReqCPUS', 'n':'ReqNodes'}
avail_sort = ['Jobs', 'Nodes', 'CPUs', 'GPUs', 'RAM']
avail_levels = ['User', 'Account', 'State', 'Partition']

def get_levels(level_string):
    levels = []
    for l in level_string.split(','):
        level_ok = False
        for avail_level in avail_levels:
            if avail_level.startswith(l.capitalize()) or  avail_level.startswith == l.capitalize():
                levels.append(avail_level)
                level_ok = True
        if not level_ok:
            sys.exit("Level not recognized: {}".format(l))
    return levels

def get_subprocess_lines(cmd):
    try:
        pipe = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        for line in pipe.stdout:
            yield line.decode().strip()
        pipe.wait()
    except OSError as e:
        print("Couldn't find slurm commands on your path. Are you sure you're on a slurm cluster?")
        sys.exit(1)

def get_job_memory(job_info):
    units = job_info['ReqMem'][-2]
    core_node = job_info['ReqMem'][-1]
    raw_memory = float(job_info['ReqMem'][:-2])
    return int(raw_memory * size_multipliers[units] * int(job_info[core_node_keys[core_node]]))

def summarize_jobs(summary_levels):
    summary = dd(lambda: {'Jobs': 0, 'CPUs': 0, 'GPUs': 0,
                          'RAM': 0, 'Nodes': 0})
    sacct_cmd = ['sacct', '-XaPsR,PD,RQ', '-oUser,Account,State,Partition,ReqCPUS,ReqNodes,ReqMem,ReqGRES']
    for i, line in enumerate(get_subprocess_lines(sacct_cmd)):
        if i == 0:
            header = line.split('|')
        else:
            job_info = dict(zip(header, line.split('|')))
            job_info['State'] = job_info['State'].lower()
            job_memory = get_job_memory(job_info)
            level_idx = tuple(job_info[x] for x in summary_levels)
            summary[level_idx]['Jobs'] += 1
            summary[level_idx]['CPUs'] += int(job_info['ReqCPUS'])
            summary[level_idx]['RAM'] += get_job_memory(job_info)
            summary[level_idx]['Nodes'] += int(job_info['ReqNodes'])
            if job_info['ReqGRES'].startswith('gpu'):
                summary[level_idx]['GPUs'] += int(job_info['ReqGRES'].split(':')[1])
    return summary

def print_summary(summary_dict, summary_levels, show_gpu, ram_units, sort_on, ascending):
    sortable_columns = avail_sort
    rows = [ ]
    if not show_gpu:
        sortable_columns.remove('GPUs')
    rows.append(summary_levels+sortable_columns)
    for level_idx, info_dict in sorted(summary_dict.items(), key=lambda x: tuple(x[1][y] for y in sort_on), reverse=ascending):
        info_dict['RAM'] = round((info_dict['RAM'] / size_multipliers[ram_units]), 1)
        rows.append([str(a) for a in level_idx+tuple(info_dict[x] for x in sortable_columns)])
    max_widths = [max(map(len, col)) for col in zip(*rows)]
    for row in rows:
        print(" ".join((val.ljust(width) for val, width in zip(row, max_widths))))


def get_args():
    parser = argparse.ArgumentParser(description="get a summary of job usage", prog='job-summary')
    parser.add_argument('-l', '--levels',
                        default='user,state',
                        help='What to summarize output on. Can specify more than one of: u user a account s state p partiton. e.g. u,s or user,state')
    parser.add_argument('-g', '--gpu',
                        action='store_true',
                        help='Show GPUs too.')
    parser.add_argument('-s', '--sort-on',
                        default=['CPUs'],
                        action='append',
                        choices=avail_sort,
                        help='What to sort output on. Can specify more than one.')
    parser.add_argument('-a', '--ascending',
                        action='store_true',
                        help='Sort in ascending order (default is descending).')
    parser.add_argument('-u', '--units',
                        default='G',
                        choices=list(size_multipliers.keys()),
                        help='What units to report memory in.')
    return vars(parser.parse_args())

if __name__ == '__main__':
    args = get_args()
    levels = get_levels(args['levels'])
    job_summary = summarize_jobs(levels)
    if 'GPUs' in args['sort_on']:
        args['gpu']=True
    print_summary(job_summary, levels, args['gpu'], args['units'], args['sort_on'], args['ascending'])
