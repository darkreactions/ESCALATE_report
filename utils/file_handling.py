import os
import re

from expworkup.devconfig import valid_input_files, workup_targets, lab_vars


def get_interface_filename(interface_type, working_directory, runID):
    for suffix in valid_input_files[interface_type]:
        filename = os.path.join(working_directory, f'{runID}_{suffix}')
        if os.path.exists(filename):
            return filename

    raise FileNotFoundError(f'Could not find any of {valid_input_files[interface_type]} file for {runID}')


def get_experimental_run_lab(run_filename):
    """

    :param run_filename: either the remote run directory name or the local json that is generated from it
    :return: the labname
    """
    for lab in lab_vars.keys():
        lab_pat = re.compile(f'_({lab})($|.json$)')
        match = lab_pat.search(run_filename.strip())
        if match:
            return match.group(1)

    raise RuntimeError(f'{run_filename} does not specify a supported lab')
