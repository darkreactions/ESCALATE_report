import builtins
import io
import os
import pytest
import pandas as pd
import shutil

from runme import parse_args
from runme import get_remote_data
from runme import report_pipeline

@pytest.fixture(scope='module')
def get_devreport_df():
    df = pd.read_csv('tests/devreport_20200504.csv', low_memory=False)
    return df

@pytest.fixture(scope='session')
def dev_args():
    parser = parse_args(['testing', '-d', 'dev', '--raw', '1'])
    return parser

@pytest.fixture(scope='session')
def chemdf_dict(dev_args):
    """
    Gather all dev inventories for later use 
    effectively stores the chemdf_dict for testing

    Generates folders for run staging
    Deletes folder tree when complete

    OK offshoot for variable testing or other inventory related testing,
    though this will be rather far from the data entry.  Good luck recovering...

    """ 
    dataset_list = dev_args.d
    target_naming_scheme = dev_args.local_directory
    offline_folder = f'./{target_naming_scheme}/offline'
    offline_toggle = 0
    target_naming_scheme = dev_args.local_directory
    log_directory = f'{target_naming_scheme}/logging'  # folder for logs

    if not os.path.exists(target_naming_scheme):
        os.mkdir(target_naming_scheme)
    if not os.path.exists(log_directory):
        os.mkdir(log_directory)
    offline_folder = f'./{target_naming_scheme}/offline'
    if not os.path.exists(offline_folder):
        os.mkdir(offline_folder)

    chemdf_dict = get_remote_data(dataset_list, 
                                  target_naming_scheme,
                                  offline_folder,
                                  offline_toggle)
    yield chemdf_dict
    shutil.rmtree(str(target_naming_scheme))
    os.remove('mycred.txt')

@pytest.fixture(scope='module')
def get_report_df(get_devreport_df, chemdf_dict, dev_args):
    """
    Grab a default devreport under the expected conditions 
    which can be used for downstream testing

    """
    dataset_list = dev_args.d
    raw_bool = dev_args.raw
    target_naming_scheme = dev_args.local_directory
    offline_folder = f'./{target_naming_scheme}/offline'
    offline_toggle = 0
    report_df = report_pipeline(chemdf_dict, 
                            raw_bool,
                            target_naming_scheme, 
                            dataset_list, 
                            offline_folder, 
                            offline_toggle)
    # pandas does stuff during read write... so mimic..
    report_df.to_csv(f'{offline_folder}/testing.csv')
    report_df = pd.read_csv(f'{offline_folder}/testing.csv')
    yield report_df

@pytest.fixture(scope='module')
def get_report_df_t(get_report_df):
   report_df = get_report_df.copy()
   report_df = report_df.set_index('name')
   report_df_t = report_df.T
   yield report_df_t
    
@pytest.fixture(scope='module')
def get_devreport_df_t(get_devreport_df):
   report_df = get_devreport_df.copy()
   report_df = report_df.set_index('name')
   report_df_t = report_df.T
   yield report_df_t