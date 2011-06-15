#!/usr/bin/env python
import sys
BLANK = '-'
num_mbrs = k = BLANK
with open(sys.argv[1]) as f:
    for line in f:
        if line.startswith('Experiment'):
            num_mbrs = k = BLANK
            print '#', line.strip()
        if line.startswith('testing with'):
            num_mbrs = line.split()[2]
        if line.startswith('k value'):
            k = line.split()[2]
        if 'NumIterations' in line:
            if line.startswith('k'):
                vals = line.split()[3::2]
            else:
                vals = line.split()[1::2]
            print num_mbrs +', ' + k + ', ' + ' '.join(vals)
