#!/usr/bin/env python2

import csv
import glob
import operator
import os.path
import string
import sys

import mincemeat as mm

fieldnames = ['matrix', 'row', 'col', 'value']


def run_server(mat_file):
    matrices = list(csv.DictReader(mat_file))
    matsize = max(int(x['row']) for x in matrices) + 1

    s = mm.Server()
    s.datasource = { (x['matrix'] == 'a', int(x['row']), int(x['col'])): (matsize, int(x['value']))
                     for x in matrices }
    s.mapfn = mapfn
    s.reducefn = reducefn

    return s.run_server()


def mapfn(k, v):
    isLeft, row, col = k
    matsize, value = v

    if isLeft:
        for j in xrange(matsize):
            yield (row, j), (isLeft, col, value)
    else:
        for i in xrange(matsize):
            yield (i, col), (isLeft, row, value)


def reducefn(k, vs):
    left_vals = [(c, val) for isLeft, c, val in vs if isLeft is True]
    right_vals = [(c, val) for isLeft, c, val in vs if isLeft is False]

    left_vals = map(lambda x: x[1], sorted(left_vals, key=lambda x: x[0]))
    right_vals = map(lambda x: x[1], sorted(right_vals, key=lambda x: x[0]))

    return reduce(lambda x, y: (x + y) % 97,
                  map(lambda (x, y): (x * y) % 97,
                      zip(left_vals, right_vals)))


if __name__ == '__main__':
    with open(sys.argv[1], 'r') as mat_file:
        matC = run_server(mat_file)

        with open(sys.argv[2], 'w') as out_file:
            writer = csv.DictWriter(out_file, fieldnames)

            writer.writeheader()

            for (row, col), value in matC.iteritems():
                writer.writerow({'matrix': 'c', 'row': row, 'col': col, 'value': value})
