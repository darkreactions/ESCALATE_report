import pandas as pd
import pytest
import os

from runme import parse_args
from tests.conftest import ARGS_LIST
from expworkup.createjson import parse_observation_interface, parse_preparation_interface, download_experiment_directories
from utils.file_handling import get_interface_filename, get_experimental_run_lab

"""
Notes on these tests
This test is a compliment to validation which exists during all runtimes of the code
EXAMPLE only. 

These lay the groundwork for a conftest fixture (downloaded files)
NOTE: many possible failures are duplicated in log files generated each run, these are just explicit errors to show during testing
if the testing log is cleaned up at the end of the run, comment out the last two lines of 
chemdf_dict fixture
"""

def kickoff_observation_validation(arguments):
    """
    gather exp_dict (list of downloaded run names) for run by run assessment
    """
    dev_args = parse_args(arguments)
    target_directory = dev_args.local_directory
    working_directory = f'./{target_directory}/gdrive_files'
    if not os.path.exists(target_directory):
        os.mkdir(target_directory)
    if not os.path.exists(working_directory):
        os.mkdir(working_directory)
    dataset = dev_args.d[0] # just do the first one... 
    exp_dict = download_experiment_directories(target_directory, dataset)
    return exp_dict, working_directory

# Have to get the files local before knowing how many there are...
exp_dict, working_directory= kickoff_observation_validation(ARGS_LIST)

def generate_name_list(default_name):
    ### Return the list of files based on the default_name
    file_list = []
    for experiment_name in exp_dict.keys():
        my_file = get_interface_filename(default_name,
                                      working_directory,
                                      experiment_name)
        file_list.append(my_file)
    return file_list

@pytest.mark.parametrize("observation_filename", generate_name_list('observation_interface'))
def test_observation_interface(observation_filename):
    # Check if the downloaded observation interfaces pass schema validation
    # This function is only as good as the schema validation
    temp, is_valid = parse_observation_interface(observation_filename)
    assert is_valid, f"{observation_filename} failed schema validation"

@pytest.mark.parametrize("preparation_fname", generate_name_list('preparation_interface'))
def test_preparation_interface(preparation_fname):
    # Check if the downloaded json backend from the preparation interface passes schema validation
    # This function is only as good as the schema validation
    temp, is_valid = parse_preparation_interface(preparation_fname)
    assert is_valid, f"{preparation_fname} failed schema validation"

