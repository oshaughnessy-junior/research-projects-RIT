#! /usr/bin/env python
# util_CleanILE.py
#
#  Reads FILE (not stdin). Consolidates ILE entries for the same physical system.
#  Compare to: util_MassGriCoalesce.py


import sys
import os

import numpy as np
import RIFT.misc.weight_simulations as weight_simulations
import scipy.special
from RIFT.misc import hyperpipeline_io

import fileinput
#import StringIO

data_at_intrinsic = {}
input_coverage = {}

my_digits=7  # safety for high-SNR BNS


import argparse
parser = argparse.ArgumentParser(usage="util_HyperCombine.py fname1.dat fname2.dat ... ")
parser.add_argument("fname",action='append',nargs='+')
parser.add_argument("--combination",default='average',help='average|sum|product, depends on if we treat them as independent trials of the same quantity or independent')
parser.add_argument("--preserve-intrinsic-values", action="store_true",
                    help="Group by exact parsed intrinsic values, without decimal rounding.")
parser.add_argument("--expected-inputs", type=int, default=None,
                    help="Strict product: require this many distinct input files and exactly "
                         "one row for every observed intrinsic point in every file.")
opts = parser.parse_args()
if opts.expected_inputs is not None:
    if opts.combination != "product" or opts.expected_inputs <= 0:
        parser.error("--expected-inputs requires --combination product and a positive count")
    paths = [os.path.realpath(f) for f in opts.fname[0]]
    if len(paths) != opts.expected_inputs or len(set(paths)) != opts.expected_inputs:
        parser.error("strict product requires {} distinct input files".format(opts.expected_inputs))
    if any(not os.path.isfile(f) or os.stat(f).st_size == 0 for f in paths):
        parser.error("strict product requires every input file to exist and be nonempty")

n_terms = 0
enforce_length=False
if opts.combination != 'average':
    enforce_length=True

# Detect the self-describing Hyperpipe format once and require every readable
# input to carry the same named columns.  Legacy files retain the historical
# first-comment-line passthrough below.
# Key the detection off the first READABLE, NON-EMPTY input, not literally
# the first argument: an empty first shard (a failed worker) would otherwise
# silently disable the mixed-input and column-mismatch checks for the whole
# merge and emit a headerless output that downstream sniffs as legacy.
first_input = next(
    (f for f in opts.fname[0]
     if os.path.exists(os.path.realpath(f))
     and os.stat(os.path.realpath(f)).st_size > 0),
    None)
if first_input is None:
    raise ValueError(
        "util_HyperCombine: no readable, non-empty input files")
hyperpipe_columns = None
if hyperpipeline_io.sniff(first_input):
    hyperpipe_columns = hyperpipeline_io.read_header(first_input)

#print opts.fname
from pathlib import Path
for fname in opts.fname[0]: #sys.argv[1:]:
    fname  = Path(fname).resolve()
    if not( os.path.exists(fname)):  # skip symbolic links that don't resolve : important for .composite files
        continue
    if os.stat(fname).st_size==0:  # skip files of zero length
        continue
    if hyperpipe_columns is not None:
        if not hyperpipeline_io.sniff(str(fname)):
            raise ValueError(
                "util_HyperCombine: mixed Hyperpipe/legacy inputs: {}".format(
                    fname))
        columns_here = hyperpipeline_io.read_header(str(fname))
        if columns_here != hyperpipe_columns:
            raise ValueError(
                "util_HyperCombine: Hyperpipe column mismatch in {}: {} != {}"
                .format(fname, columns_here, hyperpipe_columns))
    sys.stderr.write(str(fname)+"\n")
#    data = np.loadtxt(fname)  # this will FAIL if we have a heterogeneous data source!  BE CAREFUL
    data = np.genfromtxt(fname,invalid_raise=opts.expected_inputs is not None)  #  Protect against inhomogeneous data
    if opts.expected_inputs is not None and data.size == 0:
        raise ValueError("strict product: no data rows in {}".format(fname))
    if len(data.shape)==1:
        data = np.array([data])
    for line in data:
      if opts.expected_inputs is not None:
        if hyperpipe_columns is not None and len(line) != len(hyperpipe_columns):
            raise ValueError("strict product: row/header column mismatch in {}: {} != {}".format(
                fname, len(line), len(hyperpipe_columns)))
        if (len(line) < 3 or not np.all(np.isfinite(line[2:]))
                or np.isnan(line[0]) or np.isposinf(line[0])
                or not np.isfinite(line[1]) or line[1] < 0):
            raise ValueError("strict product: invalid row in {}".format(fname))
      if not opts.preserve_intrinsic_values:
        line = np.around(line, decimals=my_digits)
      if opts.expected_inputs is not None:
        sources = input_coverage.setdefault(tuple(line[2:]), set())
        if str(fname) in sources:
            raise ValueError("strict product: duplicate point {} in {}".format(tuple(line[2:]), fname))
        sources.add(str(fname))
      try:
        if tuple(line[2:]) in data_at_intrinsic:
#            print " repeated occurrence ", line[1:9]
            data_at_intrinsic[tuple(line[2:])].append(line[:2])
        else:
#            print " new key ", line[1:9]
            data_at_intrinsic[tuple(line[2:])] = [line[:2]]
        n_terms = np.max([n_terms, len(data_at_intrinsic[tuple(line[2:])]) ])
      except:
          continue

if opts.expected_inputs is not None:
    incomplete = [key for key, sources in input_coverage.items()
                  if len(sources) != opts.expected_inputs]
    if incomplete:
        raise ValueError("strict product: missing event factors at {} point(s), including {}; "
                         "exact intrinsic values must match across every input file".format(
            len(incomplete), incomplete[:3]))

if hyperpipe_columns is not None:
    print("# " + hyperpipeline_io.MAGIC)
    print("# " + " ".join(hyperpipe_columns))
else:
    header_str=''
    with open(first_input,'r') as f:
        header_str = f.readline().rstrip()
    if header_str.startswith('#'):
        print(header_str)  # repeat legacy single-line headers verbatim
for key in data_at_intrinsic:
    if len(key) < 1: 
        # sometimes we get spaces showing up, which is weird. Skip these. Should not happen
        continue
    if len(data_at_intrinsic[key]) < 1:
        # skip empty rows. This should not happen
        continue
    if enforce_length:
        if len(data_at_intrinsic[key]) < n_terms:
            print(" Incomplete data for {} ".format(key),file=sys.stderr)
            continue
    lnL, sigmaOverL =   np.transpose(data_at_intrinsic[key])
    lnLmax = np.max(lnL)
    if opts.combination =='product':
        lnLnet = np.sum(lnL)
        sigma = np.sqrt(np.sum(sigmaOverL**2))
        print(" {} {} ".format(lnLnet, sigma) + ' '.join(map(str,key)) )
    else:
        # (-inf, 0) is the producer's rejected EOS-proposal sentinel.  For
        # repeated estimates of one model, skip rejected replicas when a valid
        # estimate remains; this is not a zero-valued event factor to drop from
        # a PRODUCT.  Keep the historical finite-replica averaging unchanged.
        keep = ~(np.isneginf(lnL) & (sigmaOverL == 0))
        if not np.any(keep):
            print(" {} {} ".format(-np.inf, 0.) + ' '.join(map(str,key)))
            continue
        lnL, sigmaOverL = lnL[keep], sigmaOverL[keep]
        lnLmax = np.max(lnL)
        sigma = sigmaOverL*np.exp(lnL-lnLmax)  # remove overall Lmax factor, which factors out from the weights constructed from \sigma

        # This is an average, treating them as independent measurements
        wts = weight_simulations.AverageSimulationWeights(None, None,sigma)   
        lnLmeanMinusLmax = scipy.special.logsumexp((lnL - lnLmax)*wts)
        sigmaNetOverL = (np.sqrt(1./np.sum(1./sigma/sigma)))/np.exp(lnLmeanMinusLmax)

        if opts.combination =='sum':
            lnLmeanMinusLmax *= len(wts)   # not an average !
            sigmaNetOverL *= np.sqrt(len(wts))
                              
        print(" {} {} ".format(lnLmeanMinusLmax+lnLmax, sigmaNetOverL) + ' '.join(map(str,key)) )
