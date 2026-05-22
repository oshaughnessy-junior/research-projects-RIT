#! /usr/bin/env python
# Very very quick tool to yank out posterior quantiles from an ascii file
#  Example

#  util_CharacterizePosterior.py --parameter mc --quantiles '[0.9]'
# for i in S*.dat; do echo $i; python util_CharacterizePosterior.py --fname $i --parameter mc --quantiles [0.05,0.95] --parameter q; done



import argparse
import numpy as np
import RIFT.lalsimutils
import RIFT.misc.samples_utils as samples_utils


parser = argparse.ArgumentParser()
parser.add_argument("--fname",type=str,help="filename of *.dat file [standard ILE output]")
parser.add_argument("--parameter",action='append',help="name of param")
parser.add_argument("--quantiles",type=str,help="quantiles")
opts=  parser.parse_args()

#print opts.parameter

quantile_list = np.array(eval(opts.quantiles))
#print opts.quantiles, quantile_list

dat = samples_utils.load_posterior_samples(opts.fname)
for param in opts.parameter:
    # lightweight conversion: not full capability
    if param in dat.dtype.names:
        dat_1d = dat[param]
    else:
        # Fallback to derived parameters if not directly in the loaded samples
        # load_posterior_samples already calls standard_expand_samples, 
        # so common combinations like 'mc', 'q', 'xi' should now be present.
        try:
            dat_1d = dat[param]
        except KeyError:
            # This is a safety fallback in case the param isn't in expanded samples
            if param == 'mc':
                dat_1d = RIFT.lalsimutils.mchirp(dat['m1'],dat['m2'])
            elif param == 'q':
                dat_1d = dat['m2']/dat['m1']
            elif param == 'xi':
                dat_1d = (dat['m2']*dat['a2z']+dat['m1']*dat['a1z'])/(dat['m1']+dat['m2'])
            elif param=='mtot':
                dat_1d = dat['m1']+dat['m2']
            else:
                # Force failure for truly missing params
                dat_1d = dat[param]
    quant_here  = np.percentile(dat_1d,100*quantile_list)
    print(param, ' '.join(map(str,quant_here)))
