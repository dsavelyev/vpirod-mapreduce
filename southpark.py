#!/usr/bin/env python2

import csv
import sys

import mincemeat as mm


def run_server(csv_file):
    rdr = dict(enumerate(csv.DictReader(csv_file)))

    s = mm.Server()
    s.datasource = rdr
    s.mapfn = mapfn
    s.reducefn = reducefn

    return s.run_server()


def mapfn(k, v):
    import nltk.tokenize as tkz
    import string

    char, line = v['Character'], v['Line']
    for w in tkz.wordpunct_tokenize(line.translate(None, string.puncuation)):
        yield char, w.lower()


def reducefn(k, vs):
    return len(set(vs))


if __name__ == '__main__':
    with open(sys.argv[1]) as f:
        out = run_server(f)

    with open(sys.argv[2], 'w') as outf:
        csv.writer(outf).writerows(out.items())
