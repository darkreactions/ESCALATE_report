import pandas as pd
import pytest
from tests.conftest import TEST_TARGET
"""
Notes on testing from early development:
- modification of the the devreport file by programs such as excel can result in additional error messages (files are modified in some way)
- if changes are expected the errors can be ignored
- most of the tests are to ensure that the data present (minimum viable product) remains unchanged. 
- Modification of the test file should only be done AFTER the new features have been validated. 
- Updating the test file should be done carefully... the ground truth should be maintained!
"""
global TEST_TARGET

def test_full(get_devreport_df, get_report_df):
    """ Test for the exact structure of the report dataframe

    This should always be a pass before uploading
    This pass depends on all workup of runs matching the validated
    dataframe.  Test will fail if example folders, code upstream, or inventories
    have changed

    smaller tests will be more useful for identifying specific changes
    """
    assert get_devreport_df.equals(get_report_df), "Failed identity check (computational exact equality), see additional errors below for more info"

def dev_df_columns():
    global TEST_TARGET
    df = pd.read_csv(TEST_TARGET, low_memory=False)
    columns = list(df.columns)
    return columns

def dev_df_rows():
    global TEST_TARGET
    df = pd.read_csv(TEST_TARGET, low_memory=False, index_col='name')
    df_t = df.T
    rows = list(df_t.columns)
    return rows

@pytest.mark.parametrize("stable_col", dev_df_columns())
def test_shared_equal_columns(stable_col, get_devreport_df, get_report_df):
    """ If columns were present in previous version, make sure data is the same
    """
    assert get_devreport_df[stable_col].equals(get_report_df[stable_col]), f'{stable_col} present in stable dev report was detected different'

@pytest.mark.parametrize("stable_row", dev_df_rows())
def test_shared_equal_rows(stable_row, get_devreport_df_t, get_report_df_t):
    """ If rows were present in previous version, make sure data is the same
    """
    assert get_devreport_df_t[stable_row].equals(get_report_df_t[stable_row]), f'{stable_row} present in stable dev report was detected different'

def test_rows_equal(get_devreport_df, get_report_df):
    """ Informs about what is different in the dataset
    (rows) if extra are added the number of new will be reported with the error
    """
    num_new_rows = get_devreport_df.shape[0] - get_report_df.shape[0] # negative means you lost rows...
    assert get_devreport_df.shape[0] == get_report_df.shape[0], f'{num_new_rows} new experiments (rows) were detected in dataset'

def test_columns_equal(get_devreport_df, get_report_df):
    """ Informs about what is different in the dataset
    (columns) if extra are added the number of new will be reported with the error
    """
    num_new_cols = get_devreport_df.shape[1] - get_report_df.shape[1] # negative means you lost columns...
    assert get_devreport_df.shape[1] == get_report_df.shape[1], f'{num_new_cols} new features (columns) were detected in dataset'