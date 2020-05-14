#Copyright (c) 2020 Ian Pendleton - MIT License
import logging
import argparse as ap
import os
import sys
import datetime

import pandas as pd
import numpy as np

from utils.globals import set_debug_header, get_debug_header
from utils.file_handling import write_debug_file
from expworkup.jsonparser import json_pipeline
from expworkup.report_view import construct_2d_view
from expworkup.createjson import download_experiment_directories
from expworkup.createjson import inventory_assembly
from expworkup.ingredients.pipeline import ingredient_pipeline
from expworkup.report_calcs import calc_pipeline
from expworkup.report_feats import feat_pipeline
from expworkup import devconfig
from expworkup import googleio
from versiondata import export_to_repo
from utils import logger
from utils import globals
from utils.globals import (
    set_target_folder_name, set_log_folder, set_offline_folder,
    get_target_folder, get_log_folder, get_offline_folder
)

__version__ = 1.0 #should match latest HISTORY.md entry

def initialize(args):
    ''' Refreshes working environment - logs initialization

    Parameters
    ----------
    args : arguments from CLI

    Returns
    ----------
    None
    '''
    modlog = logging.getLogger(f'mainlog.{__name__}')
    modlog.info(args)

def get_remote_data(datasets, offline_toggle):
    """ Acquires all chemical inventories, downloads google drive files locally
    
    if offline_toggle is > 1, returns chemdfs saved by the first local iteration

    Parameters
    ----------
    datasets : list of targeted datasets
        datasets must be included in devconfig
    
    offline_folder = folder location to store generated files  
        <local directory>/offline/<my inventories>.csv should exist if 
        offline_toggle > 0
    
    offline_toggle : explicit offline toggle from CLI
        A dev toggle to bypass google downloads after a local iteration
        Requires targeting 'dev' dataset on the first iteration (to get 
        chemical inventories)
    
    Returns
    -------
    chemdf_dict : dict of pandas.DataFrames assembled from all lab inventories
        reads in all of the chemical inventories which describe the chemical 
        content from each lab used across the dataset construction

    Notes
    ------
    Some dependencies with devconfig have not been fully automated. 
    ex. the inventory must have certain header columns, see example at:
    https://docs.google.com/spreadsheets/d/1JgRKUH_ie87KAXsC-fRYEw_5SepjOgVt7njjQBETxEg/edit#gid=1755798808
    """
    if offline_toggle == 1 or offline_toggle == 0:
        chemdf_dict = {}
        for dataset in datasets:
            # File downloaded locally in the subsequent function
            exp_dict = download_experiment_directories(get_target_folder(), 
                                                       dataset)
            chemdf_dict = inventory_assembly(exp_dict, chemdf_dict)
        if offline_toggle == 1:
            for name, chemicaldf in chemdf_dict.items():
                chemicaldf.to_csv(f'{get_offline_folder()}/{name}_INVENTORY.csv')
    if offline_toggle == 2:
        chemdf_dict = {}
<<<<<<< HEAD
<<<<<<< HEAD
        (_, _, offline_files) = next(os.walk(get_offline_folder()))
=======
=======
>>>>>>> readability, streamline feats
<<<<<<< HEAD
        (_, _, offline_files) = next(os.walk(offline_folder))
=======
        (_, _, offline_files) = next(os.walk(get_offline_folder()))
>>>>>>> readability, streamline feats
<<<<<<< HEAD
=======
=======
        (_, _, offline_files) = next(os.walk(get_offline_folder()))
>>>>>>> readability, streamline feats
>>>>>>> readability, streamline feats
        inventory_files = [x for x in offline_files if 'INVENTORY' in x]
        for inventory in inventory_files:
            inventory_name = inventory.rsplit('_', 1)[0] #ex MIT_PVLab_INVENTORY.csv to MIT_PVLab
            chemdf_dict[inventory_name] = \
                            pd.read_csv(f'{get_offline_folder()}/{inventory}',
                                        low_memory=False,
                                        index_col='InChI Key (ID)')
    return chemdf_dict

def report_pipeline(chemdf_dict, raw_bool, 
                    dataset_list, offline_toggle=0):
    """ Downloads and formats target folders as local JSONs, parses JSONs to simple 2d dataframe

    Parameters
    ----------
    chemdf_dict : dict of pandas.DataFrames assembled from all lab inventories
        reads in all of the chemical inventories which describe the chemical 
        content from each lab used across the dataset construction

    raw_bool_cli : Bool, include all columns or not
        True will enable even improperly labeled columns to be exported

    target_naming_scheme : target folder for storing the run and associated data

    dataset_list : list of targeted datasets
        datasets must be included in devconfig

    offline_folder = folder location to store generated files for subsequent runs 
        <local directory>/offline/REPORT.csv should exist if offline_toggle == 2

    offline_toggle : explicit offline toggle from CLI
        A dev toggle to bypass google downloads after a local iteration
        Requires targeting 'dev' dataset on the first iteration (to get chemical inventories)

    Returns
    ----------
    report_df : pandas.DataFrame
        2d dataframe returned after parsing all content from google drive
        returned from expworkup.json_pipeline
    """

    modlog = logging.getLogger(f'mainlog.{__name__}')

    if offline_toggle == 1 or offline_toggle == 0:
        report_df = json_pipeline(get_target_folder(),
                                  raw_bool,
                                  chemdf_dict,
                                  dataset_list)
        if offline_toggle == 1:
            modlog.info(f'Writing report dataframe locally')
            report_df.to_csv(f'{get_offline_folder()}/REPORT.csv')
    if offline_toggle == 2:
        print('offline_toggle enabled, skipped inventory downloads, json downloads, json parsing')
        if not os.path.exists(get_offline_folder()):
            modlog.error('Developer offline_toggle set before downloading files.. EXITING')
            sys.exit()
        report_df = pd.read_csv(f'./{get_offline_folder()}/REPORT.csv', low_memory=False)
    return report_df

def main_pipeline(args):
    """Handles runsetup and main function calls

    Sequence is loosely as follows
    1. Create local environment for data
    2. Gather data from targeted google drive directory
    3. Perform feature calculations / gather physicochemical descriptors
    4. Data export
    """
    initialize(args)
    dataset_list = args.d
    offline_toggle = args.offline
    raw_bool = args.raw
    #Load logging information
    set_log_folder(f'{args.local_directory}/logging')  # folder for logs
    set_target_folder_name(args.local_directory)
    if not os.path.exists(args.local_directory):
        os.mkdir(args.local_directory)
    if not os.path.exists(get_log_folder()):
        os.mkdir(get_log_folder())
    #Create offline folder - always required for chemaxon/rdkit...
    set_offline_folder(f'./{get_target_folder()}/offline')
    if not os.path.exists(get_offline_folder()):
        os.mkdir(get_offline_folder())

    #Create Loggers
    main_logger = f'{get_log_folder()}/REPORT_LOG.txt' 
    warning_logger = f'{get_log_folder()}/REPORT_WARNING_LOG.txt' 
    ingredient_logger = f'{get_log_folder()}/REPORT_INGREDIENT_LOG.txt' 
    logger.setup_logger('mainlog', main_logger)
    logger.setup_logger('warning', warning_logger, level=logging.WARN, stream=True)
    logger.setup_logger('ilog', ingredient_logger) #ingredient log
    modlog = logging.getLogger(f'mainlog.{__name__}')

    # Initial reporting signaling successful code kickoff
    modlog.info(f'{dataset_list} selected as the dataset target(s) for this run')
    print(f'{dataset_list} selected as the dataset target(s) for this run')
    print(f'{len(dataset_list)} set(s) of downloads will occur, one per dataset, please be patient!')
    modlog.info(f'{len(dataset_list)} set(s) of downloads will occur, one for dataset, please be patient!')
    modlog.info(f'Developer Option: "offline_toggle" set to {offline_toggle}')

    # Gather data from targeted directory (google UID from devcongfig datasets)
    chemdf_dict = get_remote_data(dataset_list, 
                                  offline_toggle)

    report_df = report_pipeline(chemdf_dict, 
                                raw_bool,
                                dataset_list, 
                                offline_toggle)

    debug_header = f"# Report version {__version__}; Created on {datetime.datetime.now()}; Dataset(s) targeted {dataset_list}\n"
    set_debug_header(debug_header)
    if args.debug:
        # Export dataframes of initial parsing and chemical inventories for ETL to ESCALATEV3
        report_csv_filename = f'REPORT_{get_target_folder().upper()}.csv'
        write_debug_file(report_df, report_csv_filename)
        for name, chemicaldf in chemdf_dict.items():
            inventory_name = f'REPORT_{name.upper()}_INVENTORY.csv'
            write_debug_file(chemicaldf, inventory_name)

    if args.simple:
        report_df.to_csv(f'{get_target_folder()}.csv')
        if args.offline == 0: 
            os.remove('./mycred.txt') #cleanup automatic authorization
        modlog.info(f'Simple Export Enabled: No dataset augmentation will occur!')
        print(f'Simple Export Enabled: No dataset augmentation will occur!')
        print(f'Simple Export Enabled: (3/3 steps were completed)')
        modlog.info(f'Clean Exit: {get_target_folder()}.csv was generated')
        print(f'Clean Exit: {get_target_folder()}.csv was generated')
        import sys
        sys.exit()

    # Perform feature calculations / gather physicochemical descriptors
#    compound_ingredient_objects_df = ingredient_pipeline(report_df,
#                                                         chemdf_dict,
#                                                         args.debug)

    runUID_inchi_file,\
        inchi_key_indexed_features_df= feat_pipeline(get_target_folder(),
                                                     report_df,
                                                     chemdf_dict,
                                                     args.debug,
                                                     get_log_folder())

<<<<<<< HEAD
    calc_out_df = calc_pipeline(report_df,
                                compound_ingredient_objects_df,
                                chemdf_dict,
                                args.debug) 

    #calc_out_df.to_csv(f'./{args.local_directory}/offline/REPORT_CALCOUT.csv')
    #calc_out_df = pd.read_csv(f'./{args.local_directory}/offline/REPORT_CALCOUT.csv')

    # Export dataframe
    escalate_final_df = construct_2d_view(report_df,
                                          calc_out_df,
                                          inchi_key_indexed_features_df, 
                                          args.debug,
                                          args.raw)

    escalate_final_df.to_csv(f'{get_target_folder()}.csv')


    # Additional variations on dataframe export (for escalation / versioned data repo)
    if ('state' in vars(args)):
        templink = str(args.state)
        link = templink.split('.')[0] + '.link.csv'
        pass
    else:
        modlog.error('User MUST specify a stateset during version data repo\
                     upload preparation!')
        sys.exit()

    if args.verdata is not None:
        export_to_repo.prepareexport(escalate_final_df, args.state, link, args.verdata, get_target_folder())
        modlog.info(f'Exporting {args.verdata}: {args.verdata}.{get_target_folder()}.csv was generated')
        print(f'Exporting {args.verdata}: {args.verdata}.{get_target_folder()}.csv was generated')
            
    modlog.info(f'Clean Exit: {get_target_folder()}.csv was generated')
    print(f'Clean Exit: {get_target_folder()}.csv was generated')

    # Postrun cleanup
    if args.offline == 0: 
        os.remove('./mycred.txt') #cleanup automatic authorization

def parse_args(args):
    """ Isolates argparse

    Parameters
    ----------
    args : arguments from CLI

    Returns
    ---------
    Parser : argparse arguments in parsed format (namespaced, callable)
    """
    possible_targets = [x for x in devconfig.workup_targets.keys()]
    parser = ap.ArgumentParser(description='Compile 2d version of specified dataset while saving online files to target folder')
    parser.add_argument('local_directory', type=str,
                        help='Please include target folder')
    parser.add_argument('-d',
                        type=str,
                        nargs='+',
                        choices=possible_targets,
                        metavar='DATASET',
                        help="Select one or more from the following datasets: %s \
                              The dataset(s) require the correct credentials to access.\
                              ||default = 4-Data-Iodides||" %possible_targets)
    parser.add_argument('--raw', type=bool, default=False, choices=[True, False],
                        help='final dataframe is printed with all raw values\
                        included ||default = 1||')
    parser.add_argument('--verdata', type=str, 
                        help='Enter numerical value such as "0001". Generates <0001>.perovskitedata.csv output\
                        in a form ready for upload to the versioned data repo ||default = None||')
    parser.add_argument('--verdata_version', type=str, 
                        help='Enter numerical value such as "0001". Generates <0001>.perovskitedata.csv output\
                        in a form ready for upload to the versioned data repo ||default = None||')
    parser.add_argument('--state', type=str,
                        help='title of state set file to be used at the state set for \
                        this iteration of the challenge problem, no entry will result in no processing')
    parser.add_argument('--simple', type=bool, default=False, choices=[True, False],
                        help="setting to 'True' will disable reagent processing, feature augmentation,\
                              and calculations.  The code will still export a simple report dataframe." )
    parser.add_argument('--debug', type=bool, default=False, choices=[True, False],
                        help="exports all dataframe intermediates prefixed with 'REPORT_'\
                        csvfiles with default names")
    parser.add_argument('--offline', type=int, default=0, choices=[0,1,2],
                        help="|| Default = 0 || First iteration, set to '1' to save files locally \
                        second iteration, set to '2' to load local files and continue")
    return parser.parse_args(args)


if __name__ == "__main__":
    ''' Initialize the CLI and kickoff mainbody of the code

    Notes
    ------
    Code isolated to definitions to allow for testing...
    '''
    parser = parse_args(sys.argv[1:])
    main_pipeline(parser)