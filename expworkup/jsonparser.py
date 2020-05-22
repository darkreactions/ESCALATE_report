#Copyright (c) 2018 Ian Pendleton - MIT License
import json
import pandas as pd
import numpy as np
import os
from operator import itemgetter
import json
import logging

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from tqdm import tqdm 

from expworkup.handlers.cleaner import cleaner
from expworkup import googleio
from expworkup.handlers import parser
from utils.file_handling import get_experimental_run_lab
from utils import globals

modlog = logging.getLogger(f'mainlog.{__name__}')
warnlog = logging.getLogger(f'warning.{__name__}')

def renamer(dirty_df, dataset_list, raw_bool_cli):
    """Eats dirty datasets and renames them according to dataset_rename.json

    Imports the user level json file and uses the dictionary to
    specify how to rename columns in the final report_df.  This
    allows users flexibility in how the rename without having to
    modify the code

    Parameters
    ----------
    dirty_df : pandas.DataFrame of experiments
        should be the export of, no spaces or
        special characters in the column headers

    dataset_list : list of datasets included in the report_df 
        dataset_rename.json is used to determine if a report_df
        qualifies for a rename set.  If all datasets in dataset_list
        are not in a dataset_rename, this process will return dirty_df.
    
    raw_bool_cli : cli argument, 
        if True includes extended dataframe including superfluous columns
        used in data handling

    Returns
    --------
    clean_df : pandas.DataFrame with the updated column headers

    NOTE: if you want to rename columns on a combination of datasets
    that doesn't exist in dataset_rename.json, just make that combination
    and all the selected renames.
    """
    if not raw_bool_cli:
        try:
            with open("dataset_rename.json", "r") as read_file:
                rename_dict = json.load(read_file)
        except FileNotFoundError:
            modlog.error("dataset_rename.json was not found, please redownload from ESCALATE_report")
            import sys 
            sys.exit()

        for key, name_dict in rename_dict.items():
            if 'group' in key:
                if all(elem in name_dict['datasets'] for elem in dataset_list):
                    for old_name, new_name in name_dict['columns'].items():
                        dirty_df.rename(columns={old_name : new_name}, inplace=True)
    else:
        modlog.info('Renaming was turned off for this run, columns will not all follow naming scheme')
        warnlog.info('Renaming was turned off for this run, columns will not all follow naming scheme')
    
    # alter the user to columns which do not fit the orderly naming scheme
    nonconformist_columns = []
    # runid_vial is protected for renaming downstream
    expected_prefixes = ['_rxn_', '_out_', '_calc_', '_feat_', '_raw_', '_prototype_', 'runid_vial']
    for x in dirty_df.columns:
        if not any(y in x for y in expected_prefixes):
            nonconformist_columns.append(x)

    unnamed_export_file = 'UNAMED_REPORT_COLUMNS.txt'
    # Remove what's there so it's not confusing
    if os.path.exists(unnamed_export_file):
        os.remove(unnamed_export_file)
    if len(nonconformist_columns) > 0:
        modlog.info('Columns not fitting the naming scheme were written to: UNAMED_REPORT_COLUMNS.txt')
        print('Columns not fitting the naming scheme were written to: UNAMED_REPORT_COLUMNS.txt')
        print('        The USER can define the column names in dataset_rename.json')
        with open(unnamed_export_file, 'w') as my_file:
            for x in nonconformist_columns:
                print(x, file=my_file)
    clean_df = dirty_df

    return(clean_df)

def unpackJSON(target_naming_scheme, chemdf_dict):
    """
    most granular data for each row of the final CSV is the well information.
    Each well will need all associated information of chemicals, run, etc.
    Unpack those values first and then copy the generated array to each of the invidual wells
    developed enough now that it should be broken up into smaller pieces!

    Parameters
    ----------

    target_naming_scheme : target folder for storing the run and associated data.
    
    chemdf_dict : dict of pandas.DataFrames assembled from all lab inventories
        reads in all of the chemical inventories which describe the chemical 
        content from each lab used across the dataset construction

    Return
    ------   
    concat_df_raw : pd.DataFrame, all of the raw values from the processed JSON files
        Notes: unlike previous version, no additional calculations are performed,
        just parsing the files
    """
    concat_df = pd.DataFrame()
    concat_df_raw = pd.DataFrame()

    json_list = []
    for my_exp_json in sorted(os.listdir(target_naming_scheme)):
        if my_exp_json.endswith(".json"):
            json_list.append(my_exp_json)
    for my_exp_json in tqdm(json_list):
        modlog.info('(3/4) Unpacking %s' %my_exp_json)
        concat_df = pd.DataFrame()  
        #appends each run to the original dataframe
        json_fname = (os.path.join(target_naming_scheme, my_exp_json))
        experiment_dict = json.load(open(json_fname, 'r'))

        modlog.info('Parsing %s to 2d dataframe' %json_fname)
        tray_df = parser.tray_parser(experiment_dict) #generates the tray level dataframe
        concat_df = pd.concat([concat_df,tray_df], ignore_index=True, sort=True)

        #generates a well level unique ID and aligns
        runID_df=pd.DataFrame(data=[concat_df['_raw_jobserial'] + '_' + concat_df['_raw_vialsite']]).transpose()
        runID_df.columns=['runid_vial']

        #combines all operations into a final dataframe for the entire tray level view with all information
        concat_df = pd.concat([concat_df, runID_df], sort=True, axis=1)
        #Combines the most recent dataframe with the final dataframe which is targeted for export
        concat_df_raw = pd.concat([concat_df_raw,concat_df], sort=True)
    return(concat_df_raw) 

def json_pipeline(target_naming_scheme, raw_bool_cli, chemdf_dict, dataset_list):
    '''Top level json parser pipeline

    reads in the downloaded files from google drive and parses them according
    to the user defined structure.  
    
    Parameters
    ----------
    target_naming_scheme : target folder for storing the run and associated data

    raw_bool_cli : Bool, from CLI, include all columns?
        True will enable even improperly labeled columns to be exported
        proper labels can be defined in 'dataset_rename.json'
    
    chemdf_dict : dict of pandas.DataFrames assembled from all lab inventories
        reads in all of the chemical inventories which describe the chemical 
        content from each lab used across the dataset construction

    dataset_list : list of targeted datasets
        datasets must be defined in devconfig

    Returns
    -------
    cli_specified_name : final csv file name from dataset generation

    Notes
    ---------
    NOTE: unlike <1.0 versions, this jsonparser does not do _calcs_ or _feats_ 
        See report_calcs.py or report_feats.py

    NOTE: pytest will test the output of this function on the dev dataset
    '''
    print('(3/6) Dataset download complete. Unpacking JSON Files...')
    modlog.info('%s loaded with JSONs for parsing, starting' %target_naming_scheme)

    raw_df = unpackJSON(target_naming_scheme, chemdf_dict)
    renamed_raw_df = renamer(raw_df, dataset_list, raw_bool_cli)
    report_df = cleaner(renamed_raw_df, raw_bool_cli)

    report_df['name'] = raw_df['runid_vial']
    report_df.replace('null', np.nan, inplace=True)
    report_df.replace('', np.nan, inplace=True)
    report_df.replace(' ', np.nan, inplace=True)

    return(report_df)

