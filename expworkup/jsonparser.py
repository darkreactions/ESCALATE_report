#Copyright (c) 2018 Ian Pendleton - MIT License
import json
import pandas as pd
import os
from operator import itemgetter
import json
import logging

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from tqdm import tqdm 

from expworkup import googleio
from expworkup.handlers import parser
from utils.file_handling import get_experimental_run_lab
from utils import globals

modlog = logging.getLogger('report.jsonparser')

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

def cleaner(clean_df, raw_bool_cli):
    ''' Improves consistency of namespace and fills column blanks 
    
      * Removes spaces and symbols from the columns name space, 
      * Fills blanks entries based on the datatype ('null', 0, etc) 
    
    Parameters
    ----------
    clean_df : incoming dataframe from parsing the JSON file and renaming
    raw_bool_cli : cli argument, 
        if True includes extended dataframe including superfluous columns
        used in data handling

    Returns
    ---------
    squeaky_clean_df : the report_df after selected post-processing steps
        the bigly-est and cleanly-est dataset jsonparser can generate 
        (given the selected options, no refunds)


    Cleans the file in different ways for post-processing analysis
    '''

    null_list = []
    for column_type, column_name in zip(clean_df.dtypes, clean_df.columns):
        # we have to exclude all numerical values where '0' (zero) has meaning, e.g., temperature
        if column_type == 'object':
            null_list.append(column_name)
        if column_type != 'object' and 'rxn' in column_name:
            null_list.append(column_name)
        if '_out_' in column_name:
            null_list.append(column_name)
    clean_df[null_list] = clean_df[null_list].fillna(value='null')
    clean_df = clean_df.fillna(value=0)

    rxn_df = clean_df.filter(like='_rxn_')
    raw_df = clean_df.filter(like='_raw_')
    feat_df = clean_df.filter(like='_feat_') 
    out_df = clean_df.filter(like='_out_') 
    proto_df = clean_df.filter(like='_prototype_')

    if raw_bool_cli == 1:
        squeaky_clean_df = clean_df
    else:
        squeaky_clean_df = pd.concat([out_df, rxn_df,
                                      feat_df, raw_df,
                                      proto_df],
                                      axis=1)

    squeaky_clean_df.columns = map(str.lower, squeaky_clean_df.columns)
    return(squeaky_clean_df)

    

def unpackJSON(myjson_fol, chemdf_dict):
    """
    most granular data for each row of the final CSV is the well information.
    Each well will need all associated information of chemicals, run, etc.
    Unpack those values first and then copy the generated array to each of the invidual wells
    developed enough now that it should be broken up into smaller pieces!

    #TODO: Break each of these off into tables for database
    :param myjson_fol:
    :return:
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
        runID_df.columns=['RunID_vial']
        #combines all operations into a final dataframe for the entire tray level view with all information
        concat_df = pd.concat([concat_df, runID_df], sort=True, axis=1)
        #Combines the most recent dataframe with the final dataframe which is targeted for export
        concat_df_raw = pd.concat([concat_df_raw,concat_df], sort=True)
    print('JSON to CSV conversion complete!')
    return(concat_df_raw) #this contains all of the raw values from the processed JSON files.  No additional data has been calculated

def json_pipeline(myjsonfolder, raw_bool_cli, chemdf_dict, dataset_list):
    '''Top level json parser pipeline

    reads in the downloaded files from google drive and parses them according
    to the user defined structure.  
    

    :param myjsonfolder: target folder for run and the generated data
    :param raw_bool_cli: list of cli arguments 

    Returns
    ---------
    cli_specified_name : final csv file name from dataset generation

    Notes
    ---------
    Note: unlike older code this does not perform ANY calcs.  See report_calcs.py

    '''
    print('Dataset download complete. Unpacking JSON Files...')
    modlog.info('%s loaded with JSONs for parsing, starting' %myjsonfolder)

    raw_df = unpackJSON(myjsonfolder, chemdf_dict)
    renamed_raw_df = renamer(raw_df, dataset_list)
    report_df = cleaner(renamed_raw_df, raw_bool_cli)
    report_df['name'] = report_df['runid_vial']
    #TODO: add validation to dev here

    return(report_df)

