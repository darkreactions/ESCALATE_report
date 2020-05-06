import pandas as pd
import pytest

@pytest.fixture(scope='module')
def devreport_df():
    df = pd.read_csv('tests/devreport_20200504.csv', low_memory=False)
    return devreport_df

@pytest.fixture(scope='module')
def dev_args():
    parser = runme.parse_args(['dev', '-d', 'dev', '--raw', '1', '--debug', '1'])
    return parser

