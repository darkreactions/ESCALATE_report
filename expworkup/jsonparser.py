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

def renamer(dirty_df, dataset_list):
    """Eats clean datasets and renames them according to rename_list.json

    Imports the user level json file and uses the dictionary to
    specify how to rename columns in the final report_df.  This
    allows users flexibility in how the rename without having to
    modify the code

    Parameters
    ----------
    clean_df : pandas.DataFrame of experiments
        should be the export of jsonparser.cleaner, no spaces or
        special characters in the column headers
    dataset_list : list of datasets included in the report_df 
        dataset_rename.json is used to determine if a report_df
        qualifies for a rename set.  If all datasets in dataset_list
        are not in a dataset_rename, this process will return clean_df.

    Returns
    --------
    renamed_df : pandas.DataFrame with the updated column headers
    """
    try:
        with open("dataset_rename.json", "r") as read_file:
            rename_dict = json.load(read_file)
    except FileNotFoundError:
        modlog.error("dataset_rename.json was not found, please redownload from ESCALATE_report")
        import sys 
        sys.exit()

    #The default concentrations need to be clearly marked by _rxn_ 
    # Unless better concentration models are incorporated, this should remain constant
    for key, name_dict in rename_dict.items():
        if 'group' in key:
            if all(elem in name_dict['datasets'] for elem in dataset_list):
                for old_name, new_name in name_dict['columns'].items():
                    dirty_df.rename(columns={old_name : new_name}, inplace=True)

    clean_df = dirty_df
    return(clean_df)

def unpackJSON(myjson_fol, chemdf_dict):
    """
    most granular data for each row of the final CSV is the well information.
    Each well will need all associated information of chemicals, run, etc.
    Unpack those values first and then copy the generated array to each of the invidual wells
    developed enough now that it should be broken up into smaller pieces!

    Parameters
    ----------

    myjson_fol : target folder for storing the run and associated data.
        same as target_naming_scheme
    
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
    for my_exp_json in sorted(os.listdir(myjson_fol)):
        if my_exp_json.endswith(".json"):
            json_list.append(my_exp_json)
    for my_exp_json in tqdm(json_list):
        modlog.info('(3/4) Unpacking %s' %my_exp_json)
        concat_df = pd.DataFrame()  
        #appends each run to the original dataframe
        myjson = (os.path.join(myjson_fol, my_exp_json))
        workflow1_json = json.load(open(myjson, 'r'))
        #gathers all information from raw data in the JSON file
        tray_df = parser.tray_parser(workflow1_json, myjson) #generates the tray level dataframe for all wells including some calculated features
        concat_df = pd.concat([concat_df,tray_df], ignore_index=True, sort=True)
        #generates a well level unique ID and aligns
        runID_df=pd.DataFrame(data=[concat_df['_raw_jobserial'] + '_' + concat_df['_raw_vialsite']]).transpose()
        runID_df.columns=['runid_vial']
        #combines all operations into a final dataframe for the entire tray level view with all information
        concat_df = pd.concat([concat_df, runID_df], sort=True, axis=1)
        #Combines the most recent dataframe with the final dataframe which is targeted for export
        concat_df_raw = pd.concat([concat_df_raw,concat_df], sort=True)
    return(concat_df_raw) #this contains .  No additional data has been calculated

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
    Note: unlike <1.0 versions, this jsonparser does not do _calcs_ or _feats_ 
        See report_calcs.py or report_feats.py

    '''
    print('(3/6) Dataset download complete. Unpacking JSON Files...')
    modlog.info('%s loaded with JSONs for parsing, starting' %target_naming_scheme)

    raw_df = unpackJSON(target_naming_scheme, chemdf_dict)
    renamed_raw_df = renamer(raw_df, dataset_list)
    report_df = cleaner(renamed_raw_df, raw_bool_cli)
    report_df['name'] = raw_df['runid_vial']
    #TODO: add validation to dev here
    report_df.replace('null', np.nan, inplace=True)
    report_df.replace('', np.nan, inplace=True)
    report_df.replace(' ', np.nan, inplace=True)

    return(report_df)

