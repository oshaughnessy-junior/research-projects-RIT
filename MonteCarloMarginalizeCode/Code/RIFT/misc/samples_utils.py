
import numpy as np
import RIFT.lalsimutils as lalsimutils
remap_ILE_2_LI = {
 "s1z":"a1z", "s2z":"a2z", 
 "s1x":"a1x", "s1y":"a1y",
 "s2x":"a2x", "s2y":"a2y",
 "chi1_perp":"chi1_perp",
 "chi2_perp":"chi2_perp",
 "chi1":'a1',
 "chi2":'a2',
 "cos_phiJL": 'cos_phiJL',
 "sin_phiJL": 'sin_phiJL',
 "cos_theta1":'costilt1',
 "cos_theta2":'costilt2',
 "theta1":"tilt1",
 "theta2":"tilt2",
  "xi":"chi_eff", 
  "chiMinus":"chi_minus", 
  "delta":"delta", 
  "delta_mc":"delta", 
 "mtot":'mtotal', "mc":"mc", "eta":"eta","m1":"m1","m2":"m2",
  "cos_beta":"cosbeta",
  "beta":"beta",
  "LambdaTilde":"lambdat",
  "DeltaLambdaTilde": "dlambdat",
  "thetaJN":"theta_jn"}
remap_LI_to_ILE = { "a1z":"s1z", "a2z":"s2z", "chi_eff":"xi", "lambdat":"LambdaTilde", 'mtotal':'mtot', "distance":"dist", 'ra':'phi', 'dec':'theta',"phiorb":"phiref"}

remap_bilby_to_rift={'chirp_mass':'mc', 'mass_ratio':'q', 'mass_1':'m1', 'mass_2':'m2','geocent_time':'time','luminosity_distance':'distance','phase':'phiorb','chi_1_in_plane':'chi1_perp','spin_1x': 'a1x', 'spin_1y':'a1y', 'spin_2x': 'a2x','spin_2y':'a2y', 'spin_1z':'a1z', 'spin_2z':'a2z', 'chi_2_in_plane':'chi2_perp','iota':'incl','lambda_1':'lambda1', 'lambda_2':'lambda2','lambdat':'LambdaTilde','log_likelihood':'lnL'}

import numpy.lib.recfunctions as rfn

def extract_combination_from_LI(samples_LI, p):
    """
    Extracts a specific parameter or computed combination from posterior samples.

    This function reads known columns from the posterior samples. If the requested
    parameter `p` is not directly available, it attempts to compute it from standard
    quantities using predefined remapping and physics formulas (e.g., computing
    effective spin `chi_eff` from individual spin components).

    Args:
        samples_LI (np.recarray): A structured numpy array containing posterior samples.
        p (str): The name of the parameter or combination to extract.

    Returns:
        np.ndarray: An array containing the extracted or computed values for parameter `p`.
            Returns an array of zeros if the parameter cannot be accessed to avoid
            hard failures.
    """
    if p in samples_LI.dtype.names:  # e.g., we have precomputed it
        return samples_LI[p]
    if p in remap_ILE_2_LI.keys():
       if remap_ILE_2_LI[p] in samples_LI.dtype.names:
         return samples_LI[ remap_ILE_2_LI[p] ]
    if (p == 'chi_eff' or p=='xi') and 'a1z' in samples_LI.dtype.names:
         m1 = samples_LI['m1']
         m2 = samples_LI['m2']
         a1z = samples_LI['a1z']
         a2z = samples_LI['a2z']
         return (m1 * a1z + m2*a2z)/(m1+m2)
    # Return cartesian components of spin1, spin2.  NOTE: I may already populate these quantities in 'Add important quantities'
    if p == 'chiz_plus':
        print(" Transforming ")
        if 'a1z' in samples_LI.dtype.names:
            return (samples_LI['a1z']+ samples_LI['a2z'])/2.
        if 'theta1' in samples_LI.dtype.names:
            return (samples_LI['a1']*np.cos(samples_LI['theta1']) + samples_LI['a2']*np.cos(samples_LI['theta2']) )/2.
#        return (samples_LI['a1']+ samples_LI['a2'])/2.
    if p == 'chiz_minus':
        print(" Transforming ")
        if 'a1z' in samples_LI.dtype.names:
            return (samples_LI['a1z']- samples_LI['a2z'])/2.
        if 'theta1' in samples_LI.dtype.names:
            return (samples_LI['a1']*np.cos(samples_LI['theta1']) - samples_LI['a2']*np.cos(samples_LI['theta2']) )/2.
#        return (samples_LI['a1']- samples_LI['a2'])/2.
    if  'theta1' in samples_LI.dtype.names:
        if p == 's1x':
            return samples_LI["a1"]*np.sin(samples_LI[ 'theta1']) * np.cos( samples_LI['phi1'])
        if p == 's1y' :
            return samples_LI["a1"]*np.sin(samples_LI[ 'theta1']) * np.sin( samples_LI['phi1'])
        if p == 's2x':
            return samples_LI["a2"]*np.sin(samples_LI[ 'theta2']) * np.cos( samples_LI['phi2'])
        if p == 's2y':
            return samples_LI["a2"]*np.sin(samples_LI[ 'theta2']) * np.sin( samples_LI['phi2'])
        if p == 'chi1_perp' :
            return samples_LI["a1"]*np.sin(samples_LI[ 'theta1']) 
        if p == 'chi2_perp':
            return samples_LI["a2"]*np.sin(samples_LI[ 'theta2']) 
    if 'lambdat' in samples_LI.dtype.names:  # LI does sampling in these tidal coordinates
        lambda1, lambda2 = lalsimutils.tidal_lambda_from_tilde(samples_LI["m1"], samples_LI["m2"], samples_LI["lambdat"], samples_LI["dlambdat"])
        if p == "lambda1":
            return lambda1
        if p == "lambda2":
            return lambda2
    if p == 'delta' or p=='delta_mc':
        return (samples_LI['m1']  - samples_LI['m2'])/((samples_LI['m1']  + samples_LI['m2']))
    # Return cartesian components of Lhat
    if p == 'product(sin_beta,sin_phiJL)':
        return np.sin(samples_LI[ remap_ILE_2_LI['beta'] ]) * np.sin(  samples_LI['phi_jl'])
    if p == 'product(sin_beta,cos_phiJL)':
        return np.sin(samples_LI[ remap_ILE_2_LI['beta'] ]) * np.cos(  samples_LI['phi_jl'])

    if p == 'mc':
        m1v= samples_LI["m1"]
        m2v = samples_LI["m2"]
        return lalsimutils.mchirp(m1v,m2v)
    if p == 'eta':
        m1v= samples_LI["m1"]
        m2v = samples_LI["m2"]
        return lalsimutils.symRatio(m1v,m2v)

    if (p == 'chi1' or p=='a1') and 'a1x' in samples.dtype.names:
        return np.sqrt(samples['a1x']**2 + samples['a1y']**2 + samples['a1z']**2)
    if (p == 'chi2' or p=='a2') and 'a2x' in samples.dtype.names:
        return np.sqrt(samples['a2x']**2 + samples['a2y']**2 + samples['a2z']**2)
    
    if p == 'phi1':
        return np.angle(samples_LI['a1x']+1j*samples_LI['a1y'])
    if p == 'chi_pavg':
        samples = np.array([samples_LI["m1"], samples_LI["m2"], samples_LI["a1x"], samples_LI["a1y"], samples_LI["a1z"], samples_LI["a2x"], samples_LI["a2y"], samples_LI["a2z"]]).T
        with Pool(12) as pool:   
            chipavg = np.array(pool.map(fchipavg, samples))          
        return chipavg

    if p == 'chi_p':
        samples = np.array([samples_LI["m1"], samples_LI["m2"], samples_LI["a1x"], samples_LI["a1y"], samples_LI["a1z"], samples_LI["a2x"], samples_LI["a2y"], samples_LI["a2z"]]).T
        with Pool(12) as pool:   
            chip = np.array(pool.map(fchip, samples))          
        return chip

    # Backup : access lambdat if not present
    if (p == 'lambdat' or p=='dlambdat') and 'lambda1' in samples_LI.dtype.names:
        Lt,dLt = lalsimutils.tidal_lambda_tilde(samples_LI['m1'], samples_LI['m2'],  samples_LI['lambda1'], samples_LI['lambda2'])
        if p=='lambdat':
            return Lt
        if p=='dlambdat':
            return dLt

    if p == "q"  and 'm1' in samples_LI.dtype.names:
        return samples_LI["m2"]/samples_LI["m1"]

    if 'inverse(' in p:
        # Drop first and last characters
        a=p.replace(' ', '') # drop spaces
        a = a[:len(a)-1] # drop last
        a = a[8:]
        if a =='q' and 'm1' in samples_LI.dtype.names:
            return samples_LI["m1"]/samples_LI["m2"]

    print(" No access for parameter ", p)
    return np.zeros(len(samples_LI['m1']))  # to avoid causing a hard failure

def load_posterior_samples(filepath):
    """
    Loads posterior samples from a .dat file, expands them with derived parameters,
    and applies periodic wrapping to appropriate fields.
    """
    samples = np.genfromtxt(filepath, names=True, replace_space=None)
    samples = standard_expand_samples(samples)
    for name in samples.dtype.names:
        if name in lalsimutils.periodic_params:
            samples[name] = np.mod(samples[name], lalsimutils.periodic_params[name])
    return samples

def load_composite_samples(filepath, has_labels=False, composite_dtype=None, source_redshift=None, field_names=None):
    """
    Loads composite samples from a .dat file. Handles label-based and fixed-dtype loading,
    applies source redshift scaling, filters NaN likelihoods, and applies periodic wrapping.
    """
    if not has_labels:
        if composite_dtype is None:
            if field_names is None:
                raise ValueError("composite_dtype or field_names must be provided if has_labels is False")
            composite_dtype = _detect_dtype_from_field_names(field_names)
        samples = np.loadtxt(filepath, dtype=composite_dtype)
        if source_redshift:
            samples['m1'] *= 1.0 / (1.0 + source_redshift)
            samples['m2'] *= 1.0 / (1.0 + source_redshift)
    else:
        samples = np.genfromtxt(filepath, names=True)
        # Handle potential label drift from plot_posterior_corner.py logic
        if hasattr(samples, 'dtype') and samples.dtype.names:
            samples = rfn.rename_fields(samples, {'sigmalnL': 'sigmaOverL', 'sigma_lnL': 'sigmaOverL'})

    # Filter NaN likelihoods
    if 'lnL' in samples.dtype.names:
        samples = samples[~np.isnan(samples["lnL"])]

    # Apply periodic wrapping
    for name in samples.dtype.names:
        if name in lalsimutils.periodic_params:
            samples[name] = np.mod(samples[name], lalsimutils.periodic_params[name])

    return samples

def add_field(a, descr):
    """
    Returns a new structured array with additional fields.

    The contents of "a" are copied over to the appropriate fields in the new array,
    while the new fields are left uninitialized. The original array "a" is not modified.

    Args:
        a (np.ndarray): A structured numpy array.
        descr (list): A numpy type description of the new fields to add 
            (e.g., `[('field_name', float)]`).

    Returns:
        np.ndarray: A new structured array containing the original fields and the new fields.

    Raises:
        ValueError: If the input array `a` is not a structured numpy array.
    """
    if a.dtype.fields is None:
        raise ValueError("`A' must be a structured numpy array")
    b = np.empty(a.shape, dtype=a.dtype.descr + descr)
    for name in a.dtype.names:
        b[name] = a[name]
    return b


def _detect_dtype_from_field_names(field_names):
    """
    Auto-detects the appropriate composite dtype based on field names.
    
    This logic is used to determine the data type for loading composite samples
    when the field names are known (e.g., from a .dat file with headers).
    
    Args:
        field_names: List of column names
        
    Returns:
        numpy dtype for loading the samples
    """
    # Common fields across all sample types
    common_fields = [('lnL', float), ('dist', float), ('mc', float), ('q', float), ('m1', float), ('m2', float)]
    
    # Additional fields for spin-parameterized runs
    spin_fields = [('s1z', float), ('s2z', float), ('theta1', float), ('theta2', float), ('phi1', float), ('phi2', float)]
    
    # Additional fields for spin+eos runs (with tides)
    tidal_fields = [('lambdat', float), ('dlambdat', float)]
    
    # Additional fields for eccentricity
    ecc_fields = [('ecc', float), ('omega', float)]
    
    dtype_list = common_fields.copy()
    
    # Check for spin parameters
    if any('s1' in fn or 'a1' in fn or 'chi1' in fn for fn in field_names):
        dtype_list.extend(spin_fields)
    
    # Check for tidal parameters
    if 'lambdat' in field_names:
        dtype_list.extend(tidal_fields)
    
    # Check for eccentricity
    if 'ecc' in field_names:
        dtype_list.extend(ecc_fields)
    
    # Add other common parameters that might be present
    extra_params = []
    for fn in field_names:
        if fn in ['lnL', 'dist', 'mc', 'q', 'm1', 'm2', 's1z', 's2z', 'theta1', 'theta2', 'phi1', 'phi2', 'lambdat', 'dlambdat', 'ecc', 'omega']:
            continue
        # Add any other recognized parameter
        if fn not in [d[0] for d in dtype_list]:
            extra_params.append((fn, float))
    
    dtype_list.extend(extra_params)
    
    return np.dtype(dtype_list)






def standard_expand_samples(samples):
    """
    Expands a sample set by adding standard derived parameters.

    This function checks for the presence of certain base parameters and computes
    derived quantities if they are missing. This is commonly used for preparing
    samples for plotting (e.g., in `plot_posterior_corner.py`).

    Added fields may include:
    - Mass parameters: `mtotal`, `eta`, `m1`, `m2` (computed from `mc` and `q`).
    - Spin parameters: `a1x`, `a1y`, `a2x`, `a2y`, `chi1_perp`, `chi2_perp`, `chi_eff` 
      (computed from `a1`, `theta1`, `phi1`, etc.).
    - Angles: `phi1`, `phi2`, `phi12`.
    - Tidal parameters: `lambdat`, `dlambdat` (computed from `lambda1`, `lambda2`).

    Args:
        samples (np.recarray): A structured numpy array of posterior samples.

    Returns:
        np.recarray: The expanded structured array containing the original and derived fields.
    """
    if not 'mtotal' in samples.dtype.names and 'mc' in samples.dtype.names:  # raw LI samples use 
        q_here = samples['q']
        eta_here = q_here/(1+q_here)
        mc_here = samples['mc']
        mtot_here = mc_here / np.power(eta_here, 3./5.)
        m1_here = mtot_here/(1+q_here)
        samples = add_field(samples, [('mtotal', float)]); samples['mtotal'] = mtot_here
        samples = add_field(samples, [('eta', float)]); samples['eta'] = eta_here
        if not 'm1' in samples.dtype.names:
                       samples = add_field(samples, [('m1', float)]); samples['m1'] = m1_here
                       samples = add_field(samples, [('m2', float)]); samples['m2'] = mtot_here * q_here/(1+q_here)
        
    if "theta1" in samples.dtype.names and not('chi1_perp' in samples.dtype.names):
        a1x_dat = samples["a1"]*np.sin(samples["theta1"])*np.cos(samples["phi1"])
        a1y_dat = samples["a1"]*np.sin(samples["theta1"])*np.sin(samples["phi1"])
        chi1_perp = samples["a1"]*np.sin(samples["theta1"])

        a2x_dat = samples["a2"]*np.sin(samples["theta2"])*np.cos(samples["phi2"])
        a2y_dat = samples["a2"]*np.sin(samples["theta2"])*np.sin(samples["phi2"])
        chi2_perp = samples["a2"]*np.sin(samples["theta2"])

                                      
        samples = add_field(samples, [('a1x', float)]);  samples['a1x'] = a1x_dat
        samples = add_field(samples, [('a1y', float)]); samples['a1y'] = a1y_dat
        samples = add_field(samples, [('a2x', float)]);  samples['a2x'] = a2x_dat
        samples = add_field(samples, [('a2y', float)]);  samples['a2y'] = a2y_dat
        samples = add_field(samples, [('chi1_perp',float)]); samples['chi1_perp'] = chi1_perp
        samples = add_field(samples, [('chi2_perp',float)]); samples['chi2_perp'] = chi2_perp
        if not 'chi_eff' in samples.dtype.names:
            samples = add_field(samples, [('chi_eff',float)]); samples['chi_eff'] = (samples["m1"]*samples["a1z"]+samples["m2"]*samples["a2z"])/(samples["m1"]+samples["m2"])
 
    elif 'a1x' in samples.dtype.names and not 'chi1_perp' in samples.dtype.names:
        chi1_perp = np.sqrt(samples['a1x']**2 + samples['a1y']**2)
        chi2_perp = np.sqrt(samples['a2x']**2 + samples['a2y']**2)
        samples = add_field(samples, [('chi1_perp',float)]); samples['chi1_perp'] = chi1_perp
        samples = add_field(samples, [('chi2_perp',float)]); samples['chi2_perp'] = chi2_perp

        if not ('a1' in samples.dtype.names):
            chi1 = np.sqrt(samples['a1x']**2 + samples['a1y']**2 + samples['a1z']**2)
            samples = add_field(samples, [('a1', float)]); samples['a1']= chi1
        if not ('a2' in samples.dtype.names):
            chi2 = np.sqrt(samples['a2x']**2 + samples['a2y']**2 + samples['a2z']**2)
            samples = add_field(samples, [('a2', float)]); samples['a2']= chi2

        # Askold: this part will check if phi1, phi2, phi12 are present. If not, compute and add the missing ones
        phi_fields = ['phi1', 'phi2', 'phi12']
        phi_func_dict = {
            'phi1': lambda samples: np.arctan2(samples['a1x'], samples['a1y']),
            'phi2': lambda samples: np.arctan2(samples['a2x'], samples['a2y']),
            'phi12': lambda samples: samples['phi2'] - samples['phi1']
        }

        for field_name in phi_fields:
            if not (field_name in samples.dtype.names):
                samples = add_field(samples, [(field_name, float)])
                samples[field_name] = phi_func_dict[field_name](samples)

    if not('chi1' in samples.dtype.names):
        chi1 = np.sqrt(samples['a1x']**2 + samples['a1y']**2+samples['a1z']**2)
        samples = add_field(samples, [('chi1',float)])
    if not('chi2' in samples.dtype.names):
        chi2 = np.sqrt(samples['a2x']**2 + samples['a2y']**2+samples['a2z']**2)
        samples = add_field(samples, [('chi2',float)])
        
                
    if 'lambda1' in samples.dtype.names and not ('lambdat' in samples.dtype.names):
        Lt,dLt = lalsimutils.tidal_lambda_tilde(samples['m1'], samples['m2'],  samples['lambda1'], samples['lambda2'])
        samples = add_field(samples, [('lambdat', float)]); samples['lambdat'] = Lt
        samples = add_field(samples, [('dlambdat', float)]); samples['dlambdat'] = dLt


    return samples


######### MUlTIPROCESSING FUNCTIONS ############
from multiprocessing import Pool 

def fchipavg(sample):
    """
    Helper function to compute the average effective spin chi_pavg for a single sample.

    Designed for use with `multiprocessing.Pool.map`.

    Args:
        sample (tuple): A tuple containing [m1, m2, s1x, s1y, s1z, s2x, s2y, s2z].

    Returns:
        float: The computed chi_pavg value.
    """
    P=lalsimutils.ChooseWaveformParams()
    P.m1 = sample[0]
    P.m2 = sample[1]
    P.s1x = sample[2]
    P.s1y = sample[3]
    P.s1z = sample[4]
    P.s2x = sample[5]
    P.s2y = sample[6]
    P.s2z = sample[7]
    if (P.s1x == 0 and P.s1y == 0 and P.s2x == 0 and P.s2y == 0):
        chipavg = 0
    elif (P.s1x == 0 and P.s1y == 0 and P.s1z == 0) or (P.s2x == 0 and P.s2y == 0 and P.s2z == 0):
        chipavg = P.extract_param('chi_p')
    else:
        chipavg = P.extract_param('chi_pavg')
    return chipavg     

def fchip(sample):
    """
    Helper function to compute the effective spin chi_p for a single sample.

    Designed for use with `multiprocessing.Pool.map`.

    Args:
        sample (tuple): A tuple containing [m1, m2, s1x, s1y, s1z, s2x, s2y, s2z].

    Returns:
        float: The computed chi_p value.
    """
    P=lalsimutils.ChooseWaveformParams()
    P.m1 = sample[0]
    P.m2 = sample[1]
    P.s1x = sample[2]
    P.s1y = sample[3]
    P.s1z = sample[4]
    P.s2x = sample[5]
    P.s2y = sample[6]
    P.s2z = sample[7]
    chip = P.extract_param('chi_p')
    return chip  

def dump_pesummary_samples_to_file_as_rift(fname_h5,key,fname_out,no_drop=False,no_rename=False):
    """
    Converts posterior samples from a pesummary HDF5 file to the RIFT text format.

    Reads samples from the specified key in the HDF5 file, renames fields based on 
    `remap_bilby_to_rift`, and optionally drops metadata fields (e.g., SNR, recalib).

    Args:
        fname_h5 (str): Path to the input pesummary HDF5 file.
        key (str): The key within the HDF5 file where the samples are stored.
        fname_out (str): Path to the output text file.
        no_drop (bool, optional): If True, does not drop 'recalib' or 'snr' fields. Defaults to False.
        no_rename (bool, optional): If True, does not rename fields using `remap_bilby_to_rift`. Defaults to False.

    Raises:
        Exception: If the provided key is not found in the HDF5 file.

    Example:
        >>> import samples_utils
        >>> samples_utils.dump_pesummary_samples_to_file_as_rift("metafile.h5", "bilby-IMRPhenomXPHM-SpinTaylor-3",'test.dat')
       $ convert_output_format_inference2ile --posterior-samples test.dat --output-xml my.xml.gz
    """
    import h5py
    BBH = h5py.File(fname_h5, 'r')
    if not (key in BBH.keys()):
        raise Exception(" Unknown key in file ", key, fname_h5)
    post_key = 'posterior'
    if 'posterior_samples' in BBH[key]:
        post_key = 'posterior_samples'
    samples = BBH[key][post_key]
    if hasattr(BBH[key][post_key], 'dtype'):
        dtype_us = BBH[key][post_key].dtype #
    else:
        dtype_us = np.dtype(list(map( lambda x: (x,float), list(samples.keys() ))))  # old style
    npts = len(BBH[key][post_key]['mass_1'])
    # cast to conventional structure so we can call recfunctions
    samp = np.zeros(npts,dtype=dtype_us)
    for name in dtype_us.names:
        if 'snr' in name:
            continue
        if 'approximant' in name:
            continue
        samp[name] = samples[name]
    # Rename
    samp = rfn.rename_fields(samp,remap_bilby_to_rift )
    # Drop
    if not(no_drop):
      ugly_fields = [x for x in samp.dtype.names if 'recalib' in x or 'snr' in x]
      samp = rfn.drop_fields(samp,ugly_fields)

    np.savetxt(fname_out,samp,header=" ".join(samp.dtype.names) )





def apply_kerr_limit(samples, chi_max=1.0):
    """
    Filters samples to keep only those with spin magnitude <= chi_max.
    
    Handles different spin representations (a1z, a2z) or (a1, theta1, phi1, etc.)
    
    Args:
        samples: Structured numpy array of samples
        chi_max: Maximum allowed spin magnitude (default 1.0)
        
    Returns:
        Filtered samples array
    """
    keep = np.ones(len(samples), dtype=bool)
    
    # Check for cartesian spin components
    if 'a1x' in samples.dtype.names and 'a1y' in samples.dtype.names and 'a1z' in samples.dtype.names:
        chi1 = np.sqrt(samples['a1x']**2 + samples['a1y']**2 + samples['a1z']**2)
        chi2 = np.sqrt(samples['a2x']**2 + samples['a2y']**2 + samples['a2z']**2)
    # Check for spherical spin components
    elif 'a1' in samples.dtype.names and 'theta1' in samples.dtype.names:
        # For spherical representation, a1 is already the magnitude
        chi1 = samples['a1'].copy()
        if 'a2' in samples.dtype.names and 'theta2' in samples.dtype.names:
            chi2 = samples['a2'].copy()
        else:
            chi2 = np.zeros(len(samples))
    # Check for just a1z, a2z
    elif 'a1z' in samples.dtype.names and 'a2z' in samples.dtype.names:
        chi1 = samples['a1z'].copy()
        chi2 = samples['a2z'].copy()
    else:
        # No spin information available, keep all samples
        return samples
    
    keep = (chi1 <= chi_max) & (chi2 <= chi_max)
    
    return samples[keep]


def apply_downselection(samples, downselect_dict):
    """
    Applies various downselection cuts to samples based on a dictionary of parameters.
    
    Supported parameters in downselect_dict:
        - m1_min, m1_max: Minimum and maximum for m1
        - m2_min, m2_max: Minimum and maximum for m2
        - mtot_min, mtot_max: Minimum and maximum for total mass
        - mc_min, mc_max: Minimum and maximum for chirp mass
        - q_min, q_max: Minimum and maximum for mass ratio (m2/m1)
        - chi_max: Maximum spin magnitude (passes to apply_kerr_limit)
        - lnL_min: Minimum log likelihood
        - lnL_max: Maximum log likelihood
    
    Args:
        samples: Structured numpy array of samples
        downselect_dict: Dictionary of downselection parameters
        
    Returns:
        Filtered samples array
    """
    keep = np.ones(len(samples), dtype=bool)
    
    # Mass cuts
    if 'm1_min' in downselect_dict:
        keep &= samples['m1'] >= downselect_dict['m1_min']
    if 'm1_max' in downselect_dict:
        keep &= samples['m1'] <= downselect_dict['m1_max']
    if 'm2_min' in downselect_dict:
        keep &= samples['m2'] >= downselect_dict['m2_min']
    if 'm2_max' in downselect_dict:
        keep &= samples['m2'] <= downselect_dict['m2_max']
    if 'mtot_min' in downselect_dict:
        keep &= samples['mtotal'] >= downselect_dict['mtot_min']
    if 'mtot_max' in downselect_dict:
        keep &= samples['mtotal'] <= downselect_dict['mtot_max']
    if 'mc_min' in downselect_dict:
        keep &= samples['mc'] >= downselect_dict['mc_min']
    if 'mc_max' in downselect_dict:
        keep &= samples['mc'] <= downselect_dict['mc_max']
    if 'q_min' in downselect_dict:
        keep &= (samples['m2'] / samples['m1']) >= downselect_dict['q_min']
    if 'q_max' in downselect_dict:
        keep &= (samples['m2'] / samples['m1']) <= downselect_dict['q_max']
    
    # Likelihood cuts
    if 'lnL_min' in downselect_dict and 'lnL' in samples.dtype.names:
        keep &= samples['lnL'] >= downselect_dict['lnL_min']
    if 'lnL_max' in downselect_dict and 'lnL' in samples.dtype.names:
        keep &= samples['lnL'] <= downselect_dict['lnL_max']
    
    # Spin cut (Kerr limit)
    if 'chi_max' in downselect_dict:
        samples = apply_kerr_limit(samples, downselect_dict['chi_max'])
        # Combine with other cuts - apply_kerr_limit returns filtered samples
        # So we need to adjust keep accordingly
        # Actually, we should combine logically: keep = keep & (result of apply_kerr_limit)
        # But apply_kerr_limit returns a subset, so we can just return that subset at the end
        # Let's handle differently: apply filters sequentially
    
    # Apply spin cut after other cuts if chi_max is provided
    if 'chi_max' in downselect_dict:
        samples = apply_kerr_limit(samples, downselect_dict['chi_max'])
    else:
        samples = samples[keep]
    
    return samples


def apply_lnL_cut(samples, lnL_cut):
    """
    Filters samples by log likelihood cutoff.
    
    Args:
        samples: Structured numpy array of samples
        lnL_cut: Minimum log likelihood to keep
        
    Returns:
        Filtered samples array
    """
    if 'lnL' not in samples.dtype.names:
        return samples
    
    return samples[samples['lnL'] >= lnL_cut]


def load_and_prepare_samples(filepath, sample_type='posterior', field_names=None, **kwargs):
    """
    High-level wrapper that loads, expands, and filters samples.
    
    This function provides a unified interface for loading posterior or composite samples,
    automatically expanding derived parameters, and applying filters.
    
    Args:
        filepath: Path to the samples file
        sample_type: Type of samples - 'posterior' or 'composite'
        field_names: Optional list of field names (used for composite samples to auto-detect dtype)
        **kwargs: Additional arguments passed to filtering functions:
            - chi_max: Passed to apply_kerr_limit or apply_downselection
            - downselect_dict: Dictionary of downselection parameters
            - lnL_cut: Log likelihood cutoff
        
    Returns:
        Structured numpy array of prepared samples
    """
    if sample_type == 'posterior':
        samples = load_posterior_samples(filepath)
    elif sample_type == 'composite':
        samples = load_composite_samples(filepath, field_names=field_names)
    else:
        raise ValueError(f"Unknown sample_type: {sample_type}")
    
    # Apply filters
    if 'chi_max' in kwargs:
        samples = apply_kerr_limit(samples, kwargs['chi_max'])
    if 'downselect_dict' in kwargs:
        samples = apply_downselection(samples, kwargs['downselection_dict'])
    if 'lnL_cut' in kwargs:
        samples = apply_lnL_cut(samples, kwargs['lnL_cut'])
    
    return samples



if __name__ == "__main__":
    import os
    import sys
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--key", default="rift-v5PHM-calmarg",help="Key for bilby file")
    parser.add_argument("--no-drop",action='store_true',default=False)
    parser.add_argument("fname_in",default=None,help="File name of result file")
    parser.add_argument("fname_out",default=None,help="output file")
    opts=  parser.parse_args()

    dump_pesummary_samples_to_file_as_rift(opts.fname_in, opts.key, opts.fname_out,no_drop=opts.no_drop)
    


    
