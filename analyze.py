# -*- coding: utf-8 -*-
import os
import sys
import re
from collections import Counter


def parse_args(parser, commands):
    split_argv = [[]]
    for c in sys.argv[1:]:
        if c in commands.choices:
            split_argv.append([c])
        else:
            split_argv[-1].append(c)
    # Initialize namespace
    args = argparse.Namespace()
    for c in commands.choices:
        setattr(args, c, None)
    # Parse each command
    parser.parse_args(split_argv[0], namespace=args)  # Without command
    for argv in split_argv[1:]:  # Commands
        n = argparse.Namespace()
        setattr(args, argv[0], n)
        parser.parse_args(argv, namespace=n)
    return args


def statistic_pred(kg, out_dir):
    triple_counter = Counter()
    rdf_pattern = re.compile(r'<https://permid.org/(.+)>\s<(.*?)>\s.*?\s\.')
    with open(kg, 'r') as f:
        for line in f:
            match = rdf_pattern.search(line)
            if match:
                triple_counter[match.group(2)] += 1
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    kg_name = os.path.splitext(os.path.basename(kg))[0]
    with open(os.path.join(out_dir, 'stat-pred-{}.txt'.format(kg_name)), 'w') as f:
        output_format = "{}\t{}\n"
        f.write(output_format.format('Predicate', '# triple'))
        for pred, n_tri in triple_counter.items():
            f.write(output_format.format(pred, n_tri))


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--outz')
    commands = parser.add_subparsers(title='Options')

    stat_parser = commands.add_parser('stat')
    stat_parser.add_argument('--')

    cmd2_parser = commands.add_parser('cmd2')
    cmd2_parser.add_argument('--foo')

    cmd2_parser = commands.add_parser('cmd3')
    cmd2_parser.add_argument('--foo')

    args = parse_args(parser, commands)
    print(args)

    # parser = argparse.ArgumentParser()
    # parser.add_argument('--stat',
    #                     action='store_true',
    #                     help="statistic triplets in specified domain")
    # parser.add_argument('--domain',
    #                     nargs='+',
    #                     type=str,
    #                     help="domain to be statistic, support .nt/.ntriples file")
    # parser.add_argument('-o', '--output',
    #                     type=str,
    #                     help="output dir to store output")
    # parser.add_argument('-D', '--dir',
    #                     default='./',
    #                     type=str,
    #                     help="data directory")
    # args = parser.parse_args()
    # DATA_DIR = args.dir
    # if args.stat:
    #     for dm in args.domain:
    #         domain = os.path.join(DATA_DIR, dm)
    #         output_dir = args.output
    #         statistic_pred(domain, output_dir)
    #         print("Completed statistic predicates for {}, saved result to {}".format(domain, output_dir))
