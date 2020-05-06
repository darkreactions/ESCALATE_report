# Aimed at large scale unit tests where the final deliverables are known dataframes
import pandas
import pytest
import runme

@pytest.fixture
def dev_args():
    parser = runme.parse_args(['dev', '-d', 'dev', '--raw', '1', '--debug', '1'])
    return parser