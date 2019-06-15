"""A quick-and-dirty output validator.

YARPs on success. NARPs on failure.

Intended to be run after a change to ensure data is still output as expected.
"""

import argparse
import pandas as pd


parser = argparse.ArgumentParser(
    description="""detailed usage:
    To ensure debug output is unaffected by a change, run:
    python runme.py -d 1 ref_dir before the change,
    and then
    python runme.py -d 1 target_dir after the change.

    Then, use this script to compare the corresponding CSVs. That is, run:
    python outputvalidation.py <opts> ref_csv target_csv""",
    formatter_class=argparse.RawDescriptionHelpFormatter
)
parser.add_argument('ref_csv', type=str,
                    help='CSV of report output BEFORE making the change')
parser.add_argument('target_csv', type=str,
                    help='CSV of report output AFTER making the change')
parser.add_argument('--ignore', type=str, nargs='*',
                    help='Columns to ignore, i.e. columnts that you expect to have changed. ' +
                         'Must appear in both ref and target. Do NOT ignore new columns, ' +
                         'they are ignored automatically.')
args = parser.parse_args()


ref = pd.read_csv(args.ref_csv)
target = pd.read_csv(args.target_csv)


# validate that ignore columns are actually columns
if args.ignore:
    for col in args.ignore:
        if col not in target.columns:
            raise ValueError("--ignore argument {} not in target columns".format(col))
        if col not in ref.columns:
            raise ValueError("--ignore argument {} not in ref columns".format(col))

    # drop ignore columns
    ref.drop(args.ignore, axis=1, inplace=True)
    target.drop(args.ignore, axis=1, inplace=True)

# drop any new columns
drop_cols = list((set(target.columns) - set(ref.columns)))
target.drop(drop_cols, axis=1, inplace=True)


# Now that all columns we expect to differ are dropped, the dataframes should be equal
print("...Everything ok?")
output_matches = ref.equals(target)
if output_matches:
    print('YARP')
else:
    print('NARP')
