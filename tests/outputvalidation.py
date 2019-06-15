"""A quick-and-dirty output validator.

YARPs on success. NARPs on failure.

Intended to be run after a change to ensure data is still output as expected.

USAGE:
    To ensure debug output is unaffected by a change, run:
    python runme.py -d 1 ref_dir before the change,
    and then
    python runme.py -d1 target_dir after the change.

    Then, use this script to compare the corresponding CSVs That is, run:

    python outputvalidation.py ref_csv target_csv

"""
import argparse
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument('ref_csv', type=str,
                    help='CSV of report output BEFORE making the change')
parser.add_argument('target_csv', type=str,
                    help='CSV of report output AFTER making the change')
args = parser.parse_args()

ref = pd.read_csv(args.ref_csv)
target = pd.read_csv(args.target_csv)

# drop cols from target not in ref dataset: we are allowed to add columns, not modify.
drop_cols = list((set(target.columns) - set(ref.columns)))
target.drop(drop_cols, inplace=True)

# Leroy Jenkins.
print("...Everything ok?")
output_matches = ref.equals(target)
if output_matches:
    print('YARP')
else:
    print('NARP')
