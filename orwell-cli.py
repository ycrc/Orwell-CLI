#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import unicode_literals
import re
import sys
import string
import argparse
import subprocess
from os import path, access, R_OK
from textwrap import wrap
from collections import defaultdict as dd
from collections import OrderedDict as od
from itertools import chain, cycle
from bisect import bisect_left


# Constants
# some ansi shell colors
colors = dict(black='30', blue='34', cyan='36', green='32', magenta='35',
              red='31', white='37', yellow='33')
# kinds of output
char_types = ['ascii', 'utf8', 'emoji']
# sinfo and sacct commands
sinfo_cmd = ['sinfo', '--format=%all', '-a']
sinfo_parts_cmd = ['sinfo', '--format=%P', '-ha']
sinfo_feats_cmd = ['sinfo', '-ha', '--format=%f']
sacct_cmd = ['sacct', '-XaPsR', '-oJobID,JobName,User,Account,NodeList,Partition']
# split slurm output with  slurm_delim
slurm_delim = r' ?\|'
# regexes to match node names
node_regex = re.compile('(\D+)(\d+)n?(\d*)')
gpu_regex = re.compile('NodeName=([a-zA-Z\d\[\],\-]+).+Type=([\w\d]+)\W+.*')


def get_help():
    return ("""
https://github.com/ycrc/Orwell-CLI
A utility to view slurm node status and usage.
(Any arguments that accept lists expect them to be comma separated)

""")


def _wrap(s, n):
    return '\n'.join(wrap(s, n))


def get_args():
    parser = argparse.ArgumentParser(description=get_help(), prog='orwell-cli',
                                     formatter_class=argparse.RawTextHelpFormatter)

    general_args = parser.add_argument_group('General Options')
    general_args.add_argument('-l', '--legend',
                              action='store_true',
                              help='Show legend.')
    general_args.add_argument('-i', '--general-info',
                              action='store_true',
                              help='Show some additional cluster info.')
    general_args.add_argument('-y', '--glyphs',
                              default='ascii',
                              choices=char_types,
                              help=('What character set to use when displaying cluster status. Default: ascii\n' +
                                    ' Options: {}'.format(', '.join(char_types))))
    general_args.add_argument('-s', '--show',
                              default='cpu',
                              metavar='cpu|ram|both',
                              choices=['cpu', 'ram', 'both', 'job'],
                              help=_wrap(('Show proportion of allocated CPUs, RAM, both, or job layout. ' +
                                          'Order when displaying proportion of both is CPU, RAM. ' +
                                          'Showing job will assign a glyph to each job and display the ' +
                                          'last job  running on each node. Makes the most sense on clusters ' +
                                          'with exclusive node allocation.'), 70))
    general_args.add_argument('-c', '--color',
                              default='red',
                              choices=colors.keys(),
                              help=('Color to use for highlighting. Default: red\n' +
                                    ' Options: {}'.format(', '.join(colors.keys()))))

    node_args = parser.add_argument_group('Node Filters')
    node_args.add_argument('-p', '--partition',
                           metavar='partition',
                           action='append',
                           help='Highlight nodes that are members of the given partition(s), comma separated')
    node_args.add_argument('-f', '--feature',
                           metavar='feature',
                           action='append',
                           help='Highlight nodes with the given feature(s), comma separated')
    node_args.add_argument('-g', '--gpu-type',
                           metavar='gpu_type',
                           action='append',
                           help='Highlight nodes with the given gpu(s) available, comma separated')

    job_args = parser.add_argument_group('Job/User Filters')
    job_args.add_argument('-j', '--job-id',
                          action='append',
                          help='Highlight nodes where jobs with jobid(s) are running, comma separated')
    job_args.add_argument('-P', '--job-partition',
                          action='append',
                          help='Highlight nodes where jobs submitted to the given partition(s) are running, comma separated')
    job_args.add_argument('-u', '--user',
                          action='append',
                          help='Highlight nodes where the given user(s) are running jobs, comma separated')
    job_args.add_argument('-A', '--account',
                          action='append',
                          help='Highlight nodes where the given account(s) are running jobs, comma separated')
    return vars(parser.parse_args())


def get_filters(args):
    filters = dd(lambda: [])

    for filt in ['partition', 'feature', 'gpu_type', 'job_id', 'job_partition', 'user', 'account']:
        if args[filt] is not None:
            for csv in args[filt]:
                items = csv.split(',')
                filters[filt] = filters[filt] + items
    return filters


def get_subprocess_lines(cmd):
    try:
        pipe = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        for line in pipe.stdout:
            yield line.decode().strip()
        pipe.wait()
    except OSError as e:
        print("Couldn't find slurm commands. Are you sure you're on a slurm cluster?")
        sys.exit(1)


def get_slurm_dir():
    for line in get_subprocess_lines(['sacctmgr', 'show', 'configuration']):
        if line.startswith('SLURM_CONF'):
            return path.dirname(line.split()[2])


# rest of functions
def get_pad(list_of_things):
    return max(map(len, list_of_things)) + 2


def gen_job_glyphs(char_type):
    if char_type == 'ascii':
        return cycle(string.ascii_letters + string.digits)
    elif char_type == 'utf8':
        return cycle(
            string.ascii_letters + string.digits +
            '𝔸𝔹ℂ𝔻𝔼𝔽𝔾ℍ𝕀𝕁𝕂𝕃𝕄ℕ𝕆ℙℚℝ𝕊𝕋𝕌𝕍𝕎𝕏𝕐ℤ𝟙𝟚𝟛𝟜𝟝𝟞𝟟𝟠𝟡𝟘' +
            '🅐🅑🅒🅓🅔🅕🅖🅗🅘🅙🅚🅛🅜🅝🅞🅟🅠🅡🅢🅣🅤🅥🅦🅧🅨🅩❶❷❸❹❺❻❼❽❾⓿' +
            '🄰🄱🄲🄳🄴🄵🄶🄷🄸🄹🄺🄻🄼🄽🄾🄿🅀🅁🅂🅃🅄🅅🅆🅇🅈🅉1234567890'
        )
    elif char_type == 'emoji':
        return cycle(
            '⌚⌛⌨⏏⏩⏪⏫⏬⏭⏮⏯⏰⏱⏲⏳⏸⏹⏺Ⓜ▪▫▶◀◻◼◽◾☀☁☂☃☄☎☑☔☕☘☢☣☦☪☮☯☸☹☺♀♂♈♉♊♋♌♍♎♏♐♑♒♓' +
            '♠♣♥♦♨♻♿⚒⚓⚔⚕⚖⚗⚙⚛⚜⚠⚡⚪⚫⚰⚱⚽⚾⛄⛅⛈⛎⛏⛑⛓⛔⛩⛪⛰⛱⛲⛳⛴⛵⛸⛺⛽✂✅✈✉✏✒✔✖✝✡✨✳✴❄❇❌❎' +
            '❓❔❕❗❣❤➕➖➗➡➰➿⤴⤵⬅⬆⬇⬛⬜⭐⭕〰〽㊗㊙🀄🃏🅰🅱🅾🅿🆎🆑🆒🆓🆔🆕🆖🆗🆘🆙🆚🇦🇧🇨🇩🇪🇫🇬🇭🇮🇯🇰🇱🇲🇳🇴🇵🇶🇷' +
            '🇸🇹🇺🇻🇼🇽🇾🇿🈁🈂🈚🈯🈲🈳🈴🈵🈶🈷🈸🈹🈺🉐🉑🌀🌁🌂🌃🌄🌅🌆🌇🌈🌉🌊🌋🌌🌍🌎🌏🌐🌑🌒🌓🌔🌕🌖🌗🌘🌙🌚🌛🌜🌝🌞🌟🌠🌡🌤🌥🌦' +
            '🌧🌨🌩🌪🌫🌬🌭🌮🌯🌰🌱🌲🌳🌴🌵🌶🌷🌸🌹🌺🌻🌼🌽🌾🌿🍀🍁🍂🍃🍄🍅🍆🍇🍈🍉🍊🍋🍌🍍🍎🍏🍐🍑🍒🍓🍔🍕🍖🍗🍘🍙🍚🍛🍜🍝🍞🍟🍠🍡🍢' +
            '🍣🍤🍥🍦🍧🍨🍩🍪🍫🍬🍭🍮🍯🍰🍱🍲🍳🍴🍵🍶🍷🍸🍹🍺🍻🍼🍽🍾🍿🎀🎁🎂🎃🎄🎆🎇🎈🎉🎊🎋🎌🎍🎎🎏🎐🎑🎒🎓🎖🎗🎙🎚🎛🎞🎟🎠🎡🎢🎣🎤' +
            '🎥🎦🎧🎨🎩🎪🎫🎬🎭🎮🎯🎰🎱🎲🎳🎴🎵🎶🎷🎸🎹🎺🎻🎼🎽🎾🎿🏀🏁🏅🏆🏈🏉🏏🏐🏑🏒🏓🏔🏕🏖🏗🏘🏙🏚🏛🏜🏝🏞🏟🏠🏡🏢🏣🏤🏥🏦🏧🏨🏩' +
            '🏪🏫🏬🏭🏮🏯🏰🏳🏴🏵🏷🏸🏹🏺🐀🐁🐂🐃🐄🐅🐆🐇🐈🐉🐊🐋🐌🐍🐎🐏🐐🐑🐒🐓🐔🐕🐖🐗🐘🐙🐚🐛🐜🐝🐞🐟🐠🐡🐢🐣🐤🐥🐦🐧🐨🐩🐪🐫🐬🐭' +
            '🐮🐯🐰🐱🐲🐳🐴🐵🐶🐷🐸🐹🐺🐻🐼🐽🐾🐿👑👒👓👔👕👖👗👘👙👚👛👜👝👞👟👠👡👢💄💈💉💊💋💌💍💎💐💒💓💔💕💖💗💘💙💚💛💜💝💞💟💠' +
            '💡💢💣💤💥💦💧💨💫💬💭💮💯💰💱💲💳💴💵💶💷💸💹💺💻💼💽💾💿📀📁📂📃📄📅📆📇📈📉📊📋📌📍📎📏📐📑📒📓📔📕📖📗📘📙📚📛📜📝📞' +
            '📟📠📡📢📣📤📥📦📧📨📩📪📫📬📭📮📯📰📱📲📳📴📵📶📷📸📹📺📻📼📽📿🔀🔁🔂🔃🔄🔅🔆🔇🔈🔉🔊🔋🔌🔍🔎🔏🔐🔑🔒🔓🔔🔕🔖🔗🔘🔙🔚🔛' +
            '🔜🔝🔞🔟🔠🔡🔢🔣🔤🔥🔦🔧🔨🔩🔪🔫🔬🔭🔮🔯🔰🔱🔲🔳🔴🔵🔶🔷🔸🔹🔺🔻🔼🔽🕉🕊🕋🕌🕍🕎🕐🕑🕒🕓🕔🕕🕖🕗🕘🕙🕚🕛🕜🕝🕞🕟🕠🕡🕢🕣' +
            '🕤🕥🕦🕧🕯🕰🕳🕶🕷🕸🕹🖇🖊🖋🖌🖍🖤🖥🖨🖱🖲🖼🗂🗃🗄🗑🗒🗓🗜🗝🗞🗡🗨🗯🗳🗺🗻🗼🗽🗾🗿😀😁😂😃😄😅😆😉😊😋😌😍😎😏😐😑😒😓😔' +
            '😕😖😗😘😙😚😛😜😝😞😟😠😡😢😣😤😥😦😧😨😩😪😫😬😭😮😯😰😱😲😳😴😵😶😷😸😹😺😻😼😽😾😿🙀🙁🙂🙃🙄🙈🙉🙊🚀🚁🚂🚃🚄🚅🚆🚇🚈' +
            '🚉🚊🚋🚌🚍🚎🚏🚐🚑🚒🚓🚔🚕🚖🚗🚘🚙🚚🚛🚜🚝🚞🚟🚠🚡🚢🚤🚥🚦🚧🚨🚩🚪🚫🚬🚭🚮🚯🚰🚱🚲🚳🚷🚸🚹🚺🚻🚼🚽🚾🚿🛁🛂🛃🛄🛅🛋🛍🛎🛏' +
            '🛐🛑🛒🛠🛡🛢🛣🛤🛥🛩🛫🛬🛰🛳🛴🛵🛶🛷🛸🤐🤑🤒🤔🤕🤗🤢🤣🤤🤧🤨🤩🤪🤬🤮🤯🥀🥁🥂🥃🥄🥅🥇🥈🥉🥊🥋🥌🥐🥑🥒🥓🥔🥕🥖🥗🥘🥙🥚🥛🥜' +
            '🥝🥞🥟🥠🥡🥢🥣🥤🥥🥦🥧🥨🥩🥪🥫🦀🦁🦂🦃🦄🦅🦆🦇🦈🦉🦊🦋🦌🦍🦎🦏🦐🦑🦒🦓🦔🦕🦖🦗🧀🧡🧢🧣🧤🧥🧦'
        )


def gen_usage_glyphs(char_type):
    if char_type == 'ascii':
        # usage from 10-90%
        usage_values = [(x + 1) / 10.0 for x in range(9)]
        usage_chars = [str(x + 1) for x in range(9)]
    elif char_type == 'utf8':
        # decimals reflect the height of the characters
        usage_values = [0.125, 0.25, 0.375, 0.5, 0.625, 0.75, 0.875]
        usage_chars = ['▁', '▂', '▃', '▄', '▅', '▆', '▇']
    elif char_type == 'emoji':
        # chose 7 emoji to match the utf8
        usage_values = [0.125, 0.25, 0.375, 0.5, 0.625, 0.75, 0.875]
        usage_chars = [
            '🤨',  # raised eyebrow
            '🤔',  # thinking face
            '😧',  # anguished face
            '😬',  # grimmacing face
            '😣',  # perservering face
            '🤬',  # cursing face
            '😤',  # steaming face
        ]

    return od(zip(usage_values, usage_chars))


def gen_state_glyphs(char_type):
    glyphs = dict()
    if char_type == 'ascii':
        glyphs['not a node'] = ' '
        glyphs['reserved'] = 'r'
        glyphs['idle'] = 'O'
        glyphs['down'] = 'X'
    elif char_type == 'utf8':
        glyphs['not a node'] = ' '
        glyphs['reserved'] = 'r'
        glyphs['idle'] = '▢'
        glyphs['down'] = '▼'
    elif char_type == 'emoji':
        glyphs['not a node'] = '👻'  # ghost
        glyphs['idle'] = '💤'  # zzz
        glyphs['down'] = '🚧'  # roadwork sign
        glyphs['reserved'] = '🎟️'  # admission ticket
    return glyphs


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


def get_node_glyph(state, usage, state_glyphs, usage_glyphs):
    if state.startswith('mix') or state.startswith('alloc'):
        return usage_glyphs[get_closest(usage_glyphs.keys(), usage)]
    if state.startswith('idle'):
        return state_glyphs['idle']
    if state.startswith('reserv'):
        return state_glyphs['reserved']
    else:
        return state_glyphs['down']


def print_legend(show_usage, state_glyphs, usage_glyphs):
    if show_usage == "both":
        show = 'cpu,ram'
    else:
        show = show_usage
    print("Legend")
    states = sorted(state_glyphs.keys())
    pad = get_pad(states + ['node {} allocation'.format(show)])
    for state in states:
        state_out = (state + ': ').ljust(pad)
        print('{}|{}|'.format(state_out, state_glyphs[state]))
    nu = 'node {} allocation: '.format(show).ljust(pad)
    usage_legend = '|'.join([usage_glyphs[key] for key in usage_glyphs.keys()])
    print('{}|{}|'.format(nu, usage_legend))
    print('{}^1%{}100%^\n'.format(' ' * len(nu), ' ' * (len(usage_glyphs.keys()) * 2 - 7)))


def expand_node_list(node_list):
    return chain.from_iterable(_expand_hostlist(node_list))


def _expand_hostlist(node_list):
    in_bracket = p_beg = p_end = 0
    for i, c in enumerate(node_list):
        if not in_bracket and c == ',':
            yield _expand_part(node_list[p_beg:p_end])
            p_beg, p_end = i + 1, i
        p_end += 1
        in_bracket += int(c == '[') + -1 * int(c == ']')
    yield _expand_part(node_list[p_beg:p_end])


def _expand_part(p):
    if '[' in p:
        r_beg, r_end, prefix = p.index('['), p.index(']'), p[:p.index('[')]
        for sub_r in p[r_beg + 1:r_end].split(','):
            if '-' not in sub_r:
                yield prefix + sub_r
            else:
                lo, hi = sub_r.split('-', 1)
                for i in range(int(lo), int(hi) + 1):
                    yield prefix + str(i).zfill(len(lo))
    else:
        yield p


def split_node_name(node_name):
    node_match = node_regex.match(node_name)
    groups = node_match.groups()
    # if node name is like gpu02 or bigmem05
    if groups[-1] == '':
        chassis = groups[0]
        node_num = int(groups[1])
    # if node name is like c13n05
    else:
        chassis = groups[0]+groups[1]
        node_num = int(groups[2])
    return chassis, node_num


def get_gpus():
    # where to look for gres conf
    slurm_prefix = get_slurm_dir()
    gres_conf = path.join(slurm_prefix, 'gres.conf')
    if not( path.isfile(gres_conf) and access(gres_conf, R_OK)):
        yield None
    else:
        with open(gres_conf, 'r') as gres:
            for line in gres:
                gpu_match = gpu_regex.match(line)
                if gpu_match is not None:
                    groups = gpu_match.groups()
                    if groups is not None and groups[0] is not None and groups[1] is not None:
                        hostlist, gpu = groups
                        for node in expand_node_list(hostlist):
                            yield (node, gpu)


def show_general_info():
    partitions = ', '.join(sorted(set(get_subprocess_lines(sinfo_parts_cmd))))
    gpu_info = get_gpus()
    if gpu_info is None:
        gpus = 'None'
    else:
        gpus = ', '.join(set([a[1] for a in gpu_info]))
    feature_set = set()
    for feat_line in get_subprocess_lines(sinfo_feats_cmd):
        [feature_set.add(x) for x in feat_line.split(',')]
    features = ', '.join(sorted(feature_set))
    print("""Refer to https://research.computing.yale.edu/support/hpc/clusters
and this cluster's page for more info

Partitions found (* means default):
{}
       
GPU types found:
{}

Features found:
{}
""".format(partitions, gpus, features))


def add_job_info(node_info, job_glyphs, show_usage):
    job_map = {}
    for i, line in enumerate(get_subprocess_lines(sacct_cmd)):
        if i == 0:
            header = re.split(slurm_delim, line)
        else:
            sacct = dict(zip(header, re.split(slurm_delim, line)))
            for node in expand_node_list(sacct['NodeList']):
                job_id = sacct['JobID']
                if '_' in job_id:
                    job_ids = [job_id, job_id.split('_')[0]]
                else:
                    job_ids = [job_id]
                for jid in job_ids:
                    node_info[node]['job_info'][jid]['job_name'] = sacct['JobName']
                    node_info[node]['job_info'][jid]['user'] = sacct['User']
                    node_info[node]['job_info'][jid]['account'] = sacct['Account']
                    node_info[node]['job_info'][jid]['job_partition'] = sacct['Partition']
                if show_usage == 'job':
                    if job_id not in job_map:
                        job_map[job_id] = next(job_glyphs)
                    node_info[node]['glyph'] = job_map[job_id]


def get_mem_usage(free_mem, total_mem):
    if free_mem == 'N/A':
        return 0
    else:
        return (float(total_mem) - float(free_mem)) / float(total_mem)


def get_cpu_usage(aiot):
    in_use, idle, unavailable, cores = tuple(map(float, aiot.split('/')))
    return in_use / cores


def add_node_info(node_info, chassis_layout, state_glyphs, usage_glyphs, show_usage):
    for i, line in enumerate(get_subprocess_lines(sinfo_cmd)):
        if i == 0:
            header = re.split(slurm_delim, line)
        else:
            sinfo = dict(zip(header, re.split(slurm_delim, line)))
            chassis, node_num = split_node_name(sinfo['HOSTNAMES'])
            node_name = sinfo['HOSTNAMES']
            cpu_usage = get_cpu_usage(sinfo['CPUS(A/I/O/T)'])
            mem_usage = get_mem_usage(sinfo['FREE_MEM'], sinfo['MEMORY'])
            if show_usage == 'cpu':
                node_info[node_name]['glyph'] = get_node_glyph(sinfo['STATE'], cpu_usage,
                                                               state_glyphs, usage_glyphs)
            elif show_usage == 'ram':
                node_info[node_name]['glyph'] = get_node_glyph(sinfo['STATE'], mem_usage,
                                                               state_glyphs, usage_glyphs)
            elif show_usage == 'both':
                node_info[node_name]['glyph'] = (get_node_glyph(sinfo['STATE'], cpu_usage,
                                                                state_glyphs, usage_glyphs) +
                                                 get_node_glyph(sinfo['STATE'], mem_usage,
                                                                state_glyphs, usage_glyphs))

            if chassis_layout[chassis] < node_num:
                chassis_layout[chassis] = node_num
            node_info[node_name]['partition'].add(sinfo['PARTITION'])
            [node_info[node_name]['feature'].add(f) for f in sinfo['AVAIL_FEATURES'].split(',')]


def add_gpu_info(node_info):
    gpu_info = get_gpus()
    if gpu_info is None:
        pass
    else:
        for (nodelist, gpu) in gpu_info:
            for node in expand_node_list(nodelist):
                node_info[node]['gpu_type'].add(gpu)


def get_cluster_info(state_glyphs, usage_glyphs, job_glyphs, show_usage):
    chassis_layout = dd(lambda: 1)
    node_info = dd(lambda: {'glyph': state_glyphs['not a node'], 'partition': set(),
                            'feature': set(), 'gpu_type': set(),
                            'job_info': dd(lambda: {'job_name': '',
                                                    'user': '',
                                                    'account': '',
                                                    'job_partition': ''}),
                            })

    add_job_info(node_info, job_glyphs, show_usage)
    add_node_info(node_info, chassis_layout, state_glyphs, usage_glyphs, show_usage)
    add_gpu_info(node_info)
    return(node_info, chassis_layout)


def _filter(bool_list):
    if len(bool_list) ==0:
        return False
    else:
        return(all(bool_list))


## work here
def filter_node(node, filters, nodename):
    bools = []
    for node_filter in ['partition', 'feature', 'gpu_type']:
        if node_filter in filters:
            for filt in filters[node_filter]:
                bools.append(filt in node[node_filter])
    if 'job_id' in filters:
        for job_id in filters['job_id']:
            bools.append(job_id in node['job_info'])
    
            for job_filter in ['job_partition', 'user', 'account']:
                if job_filter in filters:
                    for filt in filters[job_filter]:
                        bools.append(filt in node['job_info'][job_id][job_filter])
    else:
        for job_filter in ['job_partition', 'user', 'account']:
            if job_filter in filters:
                for filt in filters[job_filter]:
                    bools.append(any([filt in node['job_info'][job][job_filter] for job in node['job_info']]))

    return(_filter(bools))


def highlight_node(text, color):
    return u'\u001b[{}m{}\u001b[0m'.format(color, text)


def print_node_layout(node_info, chassis, filters, state_glyphs, show_usage, highlight_color):
    chas_pad = get_pad(chassis.keys())
    for chas in sorted(chassis.keys()):
        print((chas + ': ').ljust(chas_pad), end='')
        line = []
        for n in range(1, chassis[chas] + 1):
            if chas.startswith('c'):
                node = '{}n{:02d}'.format(chas, n)
            else:
                node = '{}{:02d}'.format(chas, n)
            if show_usage == 'both' and node_info[node]['glyph'] == state_glyphs['not a node']:
                node_info[node]['glyph'] += state_glyphs['not a node']
            highlight = False
            if len(filters) > 0:
                highlight = filter_node(node_info[node], filters, node)
            if highlight:
                line.append(highlight_node(node_info[node]['glyph'], colors[highlight_color]))
            else:
                line.append(node_info[node]['glyph'])
        print(u'|{}|'.format(u'|'.join(line)))


def show_cluster_info(args, filters):
    state_glyphs = gen_state_glyphs(args['glyphs'])
    usage_glyphs = gen_usage_glyphs(args['glyphs'])
    job_glyphs = gen_job_glyphs(args['glyphs'])
    if args['general_info']:
        show_general_info()
    if args['legend']:
        print_legend(args['show'], state_glyphs, usage_glyphs)

    # get node/partition/job info
    node_info, chassis_layout = get_cluster_info(state_glyphs, usage_glyphs, job_glyphs, args['show'])
    # print node layout
    print_node_layout(node_info, chassis_layout, filters, state_glyphs, args['show'], args['color'])


# Main
if __name__ == '__main__':
    args = get_args()
    filters = get_filters(args)
    show_cluster_info(args, filters)
