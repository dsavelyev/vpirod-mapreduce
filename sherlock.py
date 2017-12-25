#!/usr/bin/env python2

import csv
import glob
import os.path
import string
import sys

import mincemeat as mm


def readall(fname):
    with open(fname, 'r') as f:
        return f.read()


def run_server(docs, docnames):
    rdr = {docname: readall(fname).translate(None, string.punctuation) for fname, docname in zip(docs, docnames)}

    s = mm.Server()
    s.datasource = rdr
    s.mapfn = mapfn
    s.reducefn = eval(reducefn_template % docnames)

    return s.run_server()


def mapfn(k, v):
    import nltk.tokenize as tkz

    for w in tkz.wordpunct_tokenize(v):
        yield w.lower(), k


reducefn_template = 'lambda k, vs: map(vs.count, %s)'


if __name__ == '__main__':
    docs = glob.glob(os.path.join(sys.argv[1], '*'))
    docnames = [os.path.splitext(os.path.basename(x))[0] for x in docs]

    out = run_server(docs, docnames)

    with open(sys.argv[2], 'w') as outf:
        wr = csv.writer(outf)
        wr.writerow(['Term'] + docnames)
        wr.writerows([[k] + v for k, v in out.iteritems()])
