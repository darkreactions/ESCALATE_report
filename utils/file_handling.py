import os
import re

from expworkup.devconfig import valid_input_files, workup_targets, lab_vars
from utils.globals import get_debug_header

def get_interface_filename(interface_type, working_directory, runID):
    """ Searches for filename match and returns instance

    Specified in devconfig['valid_input_files'] 
    new file names (suffixes) can be added in devconfig as needed

    Parameters
    ----------
    working_directory : (aka save_directory) where local files are
        report default = {target_directory}/gdrive_files
    
    runID :  name of gdrive folder containing the experiment
        aka. experiment_name,  e.g. 2019-09-18T20_27_33.741387+00_00_LBL

    Returns
    -------
    filename : identified filename for a particular type of file
        e.g. type = 'experiment_specification' could be 
                    'ExperimentSpecification.xls' or 'RobotInput.xls'
    """
    for suffix in valid_input_files[interface_type]:
        filename = os.path.join(working_directory, f'{runID}_{suffix}')
        if os.path.exists(filename):
            return filename

    raise FileNotFoundError(f'Could not find any of {valid_input_files[interface_type]} file for {runID}')

def get_experimental_run_lab(run_filename):
    """ parses experiment foldername and returns lab

    Parameters
    ----------
    run_filename: either the remote run directory name or the local json that is generated from it

    Returns
    -------
    labname
    """
    for lab in lab_vars.keys():
        lab_pat = re.compile(f'_({lab})($|.json$)')
        labname = lab_pat.search(run_filename.strip()) #returns if match
        if labname:
            return labname.group(1)

    raise RuntimeError(f'{run_filename} does not specify a supported lab')

def write_debug_file(df, filename):
    if os.path.isfile(filename):
        os.remove(filename)
    f = open(filename, 'a')
    f.write(get_debug_header())
    df.to_csv(f)
    f.write(get_debug_header())
    f.close()

