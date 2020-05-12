import pandas as pd
import pytest

"""
Notes on these tests
These are a compliment to validation which exists during all runtimes of the code

Validation exists to tell the user if something is failing
Testing here is to show the developer is they are breaking the expected behavior of the code

These are by no means comprehensive
"""



#@pytest.mark.parametrize("stable_col", dev_df_columns())
#def test_shared_equal_columns(stable_col, get_devreport_df, get_report_df):
#    """ If columns were present in previous version, make sure data is the same
#    """
#    assert get_devreport_df[stable_col].equals(get_report_df[stable_col]), f'{stable_col} present in stable dev report was detected different'
#
#@pytest.mark.parametrize("stable_row", dev_df_rows())
#def test_shared_equal_rows(stable_row, get_devreport_df_t, get_report_df_t):
#    """ If rows were present in previous version, make sure data is the same
#    """
#    assert get_devreport_df_t[stable_row].equals(get_report_df_t[stable_row]), f'{stable_row} present in stable dev report was detected different'
#
#def test_rows_equal(get_devreport_df, get_report_df):
#    """ Informs about what is different in the dataset
#    (rows) if extra are added the number of new will be reported with the error
#    """
#    num_new_rows = get_devreport_df.shape[0] - get_report_df.shape[0] # negative means you lost rows...
#    assert get_devreport_df.shape[0] == get_report_df.shape[0], f'{num_new_rows} experiments (rows) were detected in dataset'
#
#def test_columns_equal(get_devreport_df, get_report_df):
#    """ Informs about what is different in the dataset
#    (columns) if extra are added the number of new will be reported with the error
#    """
#    num_new_cols = get_devreport_df.shape[1] - get_report_df.shape[1] # negative means you lost columns...
#    assert get_devreport_df.shape[1] == get_report_df.shape[1], f'{num_new_cols} new features (columns) were detected in dataset'