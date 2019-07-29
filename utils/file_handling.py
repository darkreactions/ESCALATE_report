import os

from expworkup.devconfig import valid_input_files


def get_interface_filename(interface_type, working_directory, runID):
    for suffix in valid_input_files[interface_type]:
        filename = os.path.join(working_directory, f'{runID}_{suffix}')
        if os.path.exists(filename):
            return filename

    raise FileNotFoundError(f'Could not find any of {valid_input_files[interface_type]} file for {runID}')
