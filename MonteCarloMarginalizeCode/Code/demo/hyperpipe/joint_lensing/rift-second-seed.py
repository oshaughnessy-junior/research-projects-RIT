import sys,runpy
from pathlib import Path
import numpy as np
root=Path('/home/oshaughn/rift-lensing-prototype/work/20261029-hyperpipe-native-runtime')
out=root/'gaussian-seed2'
out.mkdir(exist_ok=False)
np.random.seed(991)
script='/home/oshaughn/rift-lensing-prototype/external/rift-o4d-03d7fe8/MonteCarloMarginalizeCode/Code/bin/util_ConstructEOSPosterior.py'
sys.argv=[script,'--fname',str(root/'gaussian/evaluated.dat'),'--parameter','u_source_x','--parameter','u_lens_y','--integration-parameter-range','u_source_x:[0,1]','--integration-parameter-range','u_lens_y:[0,1]','--fit-method','rf','--rf-seed','991','--sampler-method','AV','--internal-use-lnL','--n-max','100000','--n-step','10000','--n-eff','2000','--no-plots','--fname-output-integral',str(out/'native-result'),'--fname-output-samples',str(out/'native-result')]
runpy.run_path(script,run_name='__main__')
