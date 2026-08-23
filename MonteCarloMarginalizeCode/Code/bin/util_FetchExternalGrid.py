#! /usr/bin/env python
#
#
# INTENT
#    - retrieve external grid(s), deployed in target format, and merged
#    - used to create cross-run dependencies in iterative structure

# SPECIFICATION: argument file

# * json file
#   label
#   method    : file
#     label
#     command : some methods require commands
#     arguments  : command line arguments.  Note this will not include a target name, which must be allowed as the next argument
#   convert   : None, or block
#     label
#     command
#     arguments
#
# Example
#   {
    #   "method" : "native",
    #   "source":  rundir   # will take latest grid directly, verbatim
    #   "n_max": 1000   # cap on number of points taken
    # }
    # }
# target methods
#    - copy (copy existing file)
#    - copy_latest (copy last item matching pattern, using numerical sort)



# **FETCH SPECIFICATION**


# * json file.  Default is ONE JSON PER EXECUTABLE.
#   label
#   method    : file
#     label
#     command : some methods require commands
#     arguments  : command line arguments
#   convert   : None, or block
#     label
#     command
#     arguments
   

from RIFT.misc.samples_utils import add_field,extract_combination_from_LI
import argparse
import sys
import numpy as np
import numpy.lib.recfunctions
import functools
import itertools
import os

import json
import shutil, glob,re


def retrieve_native(sourcedir,outfile,n_max=None,base_pattern=None,verbose=True):
    """
    retrieve_native(sourcedir,outfile)

    sourcedir : source directory.  Looks for last file of form "output-grid-N.xml.gz
    outfile : target file name, assume it is full path
    n_max :

    Hyperpipeline ASCII grids (opt-in via RIFT_HYPERPIPELINE_FORMAT) are
    handled automatically: the default base_pattern flips to
    "overlap-grid-*.dat", and the n_max truncation path round-trips via
    hyperpipeline_io.read_grid_to_P_list / write_grid_from_P_list instead
    of the XML helpers.
    """
    from RIFT.misc import hyperpipeline_io as _hpio
    _hpip = _hpio.is_active()
    if base_pattern is None:
        base_pattern = "overlap-grid-*.dat" if _hpip else "overlap-grid-*.xml.gz"

    if verbose:
        print("Checking ", sourcedir, " for ", base_pattern)
    # Identify the correct source file in the directory
    fnames = glob.glob(sourcedir+"/"+base_pattern)  # give flexibility to naming/reuse of this code
    if not fnames:
        raise RuntimeError(
            "No external grids matching {!r} in {!r}".format(
                base_pattern, sourcedir))

    def _numeric_basename_key(fname):
        numbers = re.findall(r"\d+", os.path.basename(fname))
        return tuple(int(number) for number in numbers)

    # Never include digits from the parent run directory in the ordering.
    fnames.sort(key=_numeric_basename_key)
    # if verbose:
    #     print(fnames)
    fname_to_use = fnames[-1]

    # If n_max is not None, load in the file, truncate its size
    if n_max is None:
        if verbose:
            print(" Transferring ", fname_to_use, " -> ", outfile)
        shutil.copyfile(fname_to_use, outfile)
    elif n_max > 0:
        import random
        import RIFT.lalsimutils as lalsimutils

        def _capped(P_list):
            """Take at most n_max points, without failing when there are fewer.

            Both branches previously computed ``P_list_reduced`` and then wrote
            the FULL list, so ``n_max`` was inert -- and
            create_event_parameter_pipeline_BasicIteration sets it to 3000 on
            the external-fetch subdag, so this is a production path.  Before
            PR #181 the key was never even passed through from the JSON, so the
            cap has never actually applied; honouring it is a BEHAVIOUR CHANGE
            for --external-fetch-native-from with grids larger than the cap.

            ``random.sample`` raises when asked for more than the population,
            which is why the size check is here and not left to it.
            """
            limit = int(n_max)
            if len(P_list) <= limit:
                return P_list
            if verbose:
                print(" Capping external grid at ", limit, " of ", len(P_list))
            return random.sample(P_list, limit)

        if _hpip or _hpio.sniff(fname_to_use):
            import lal as _lal_mod
            P_list, _columns = _hpio.read_grid_to_P_list(
                fname_to_use,
                P_factory=lalsimutils.ChooseWaveformParams,
                lal_module=_lal_mod,
                valid_params=lalsimutils.valid_params)
            _hpio.write_grid_from_P_list(outfile, _capped(P_list), _columns,
                                         lal_module=_lal_mod,
                                         lalsimutils_module=lalsimutils)
        else:
            # Load in grid
            P_list = lalsimutils.xml_to_ChooseWaveformParams_array(fname_to_use)
            lalsimutils.ChooseWaveformParams_array_to_xml(
                _capped(P_list), outfile)
    else:
        print(" Invalid fetch size ", n_max)
        import sys; sys.exit(99)
    return None


parser = argparse.ArgumentParser()
parser.add_argument("--input-json",type=str,default="fetch.json",help="input file")
parser.add_argument("--inj-file-out",type=str,default="merged_grid",help="output file")
opts=  parser.parse_args()


rundir = os.getcwd()

config=None
with open(opts.input_json,'r') as f:
    config = json.load(f)

method = config['method']
if method =='native':
    retrieve_native(
        config['source'], opts.inj_file_out,
        n_max=config.get('n_max'),
        base_pattern=config.get('base_pattern'))
