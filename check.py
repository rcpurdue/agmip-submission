#!/usr/bin/env python3
# check.py - Verify AgMIP Submission input files, rcampbel, Sept 2021

import sys
import pandas as pd

RULE_FILE = 'workingdir/RuleTables.xlsx'
TABLE = 'Table'


def header(msg):
    print()
    print(msg)
    print('=' * len(msg))


if __name__ == "__main__":
    # TODO Validiate var-unit combos

    header('Loading Rules')

    rules = pd.read_excel(RULE_FILE, engine='openpyxl', keep_default_na=False, sheet_name=None, usecols='A:B')
    valid = {'Variable': [], 'Item': [], 'Unit': [], 'Year': [], 'Region': []}

    for name in valid.keys():
        print(name)
        valid[name] = rules[name+TABLE][name]

    header('Loading Data')

    try:
        df = pd.read_csv(sys.argv[1], warn_bad_lines=True, verbose=True)
    except pd.errors.ParserError as e:
        print()
        print('ERROR(S)')
        print('========')
        print('pd.errors.ParserError: '+str(e))
        exit()

    header('Missing Data')

    nans = df.isnull().any(axis=1)
    print(df[nans])

    header('Unique Values')

    for col in df.columns:
        print(col+str(df[col].unique()))

    header('Mapping Columns')

    for arg in sys.argv[2:]:
        print(arg.split(',')[0]+' --> '+arg.split(',')[1])
        df.rename(columns={arg.split(',')[0]: arg.split(',')[1]}, inplace=True)

    header('Unrecognized Values')  # TODO Map columns from data to validation

    for col in df.columns:

        if col in valid.keys():
            invalid = []

            for test in df[col].unique():

                if test not in valid[col].values:
                    invalid.append(test)

            print(col+': '+str(invalid))
