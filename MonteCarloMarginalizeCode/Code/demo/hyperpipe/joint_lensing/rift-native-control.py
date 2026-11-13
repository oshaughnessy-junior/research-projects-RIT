import json
from pathlib import Path
import numpy as np
import RIFT.hyperpipe.drivers
root=Path('/home/oshaughn/rift-lensing-prototype/work/20261029-hyperpipe-native-runtime')
RIFT.hyperpipe.drivers.__path__.insert(0,str(root))
from RIFT.hyperpipe.drivers.joint_lensing import build_workflow,JointLensingDriver
for model,factory in [('constant','constant_factory'),('gaussian','gaussian_factory')]:
 manifest={'model':'lensed','factory':'check_joint_lensing:'+factory,'coordinates':{'source_x':{'min':2,'max':8},'lens_y':{'min':.2,'max':20,'prior':'log_uniform'}}}
 out=root/model
 build_workflow(manifest,out,n_seed=4096,seed=347)
 JointLensingDriver().run(['--joint-config',str(out/'joint.json'),'--using-eos',str(out/'seed.dat'),'--eos_start_index','0','--eos_end_index','4096','--outdir',str(out),'--fname-output-integral','evaluated.dat'])
 print(out)
