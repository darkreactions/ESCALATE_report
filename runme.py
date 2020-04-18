#Copyright (c) 2018 Ian Pendleton - MIT License
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
from expworkup.createjson import download_experiment_directories
from expworkup.createjson import inventory_assembly
from expworkup.ingredients.pipeline import ingredient_pipeline
from expworkup.report_calcs import calc_pipeline
from expworkup.report_feats import feat_pipeline
from expworkup import devconfig
from expworkup import googleio
from versiondata import export_to_repo
from tests import logger
from utils import globals

__version__ = 1.0 #should match latest HISTORY.md entry

def initialize(args):
    ''' Refreshes working environment - logs initialization

    '''
    modlog = logging.getLogger(f'mainlog.{__name__}')
    modlog.info(args)

if __name__ == "__main__":
    ''' Initialize the main body of the code

    '''
    parser = ap.ArgumentParser(description='Target Folder')
    parser.add_argument('local_directory', type=str,
                        help='Please include target folder')
    parser.add_argument('-d',
                        type=str,
                        nargs='+',
                        choices=[dataset for dataset in devconfig.workup_targets.keys()],
                        help="Please specify one or more supported datasets from the options \
                              listed. The dataset(s) require the correct credentials to access.\
                              ||default = 4-Data-Iodides||",
                        default='4-Data-Iodides'
                        )
    parser.add_argument('--raw', type=bool, default=False, choices=[True, False],
                        help='final dataframe is printed with all raw values\
                        included ||default = 1||')
    parser.add_argument('--verdata', type=str, 
                        help='Enter numerical value such as "0001". Generates <0001>.perovskitedata.csv output\
                        in a form ready for upload to the versioned data repo ||default = None||')
    parser.add_argument('--state', type=str,
                        help='title of state set file to be used at the state set for \
                        this iteration of the challenge problem, no entry will result in no processing')
    parser.add_argument('--debug', type=bool, default=False, choices=[True, False],
                        help="exports all dataframe intermediates prefixed with 'REPORT_'\
                        csvfiles with default names")

    args = parser.parse_args()

    #Load logging information
    log_directory = f'{args.local_directory}/logging'  # folder for logs
    if not os.path.exists(args.local_directory):
        os.mkdir(args.local_directory)
    if not os.path.exists(log_directory):
        os.mkdir(log_directory)

    main_logger = f'{log_directory}/REPORT_LOG.txt' 
    warning_logger = f'{log_directory}/REPORT_WARNING_LOG.txt' 
    ingredient_logger = f'{log_directory}/REPORT_INGREDIENT_LOG.txt' 
    logger.setup_logger('mainlog', main_logger)
    logger.setup_logger('warning', main_logger, level=logging.WARN, stream=True)
    logger.setup_logger('ilog', ingredient_logger) #ingredient log
    modlog = logging.getLogger(f'mainlog.{__name__}')
    warnlog = logging.getLogger(f'warning.{__name__}')
    ingredlog = logging.getLogger(f'ilog.{__name__}')

    dataset_list = args.d
    target_naming_scheme = args.local_directory

    # Initial reporting signaling successful code kickoff
    modlog.info(f'{dataset_list} selected as the dataset target(s) for this run')
    print(f'{dataset_list} selected as the dataset target(s) for this run')
    print(f'{len(dataset_list)} set(s) of downloads will occur, one per dataset, please be patient!')
    modlog.info(f'{len(dataset_list)} set(s) of downloads will occur, one for dataset, please be patient!')

    initialize(args)

    # A dev toggle to bypass google downloads after a local iteration
    # Requires targeting 'dev' dataset on the first iteration (to get chemical inventories)
    offline_toggle = 1
    # First iteration, set to '1' to save files locally
    # Second iteration, set to '2' to load local files and continue    
    offline_folder = f'./{args.local_directory}/offline'
    modlog.info(f'Developer Option: "offline_toggle" set to {offline_toggle}')
    if offline_toggle == 1 or offline_toggle == 0:
        # Always create offline folder to store cxcalc outputs
        if not os.path.exists(offline_folder):
            os.mkdir(offline_folder)
        for dataset in dataset_list:
            exp_dict = download_experiment_directories(target_naming_scheme, dataset)
            chemdf_dict = inventory_assembly(exp_dict)
        report_df = json_pipeline(target_naming_scheme,
                                  args.raw,
                                  chemdf_dict,
                                  dataset_list)
        report_df.replace('null', np.nan, inplace=True)
        report_df.replace('', np.nan, inplace=True)
        report_df.replace(' ', np.nan, inplace=True)
        if offline_toggle == 1:
            modlog.info(f'Writing intermediate files locally, ensuring {offline_folder} exists')
            report_df.to_csv(f'{offline_folder}/REPORT.csv')
            for name, chemicaldf in chemdf_dict.items():
                chemicaldf.to_csv(f'{offline_folder}/{name}.csv')
    if offline_toggle == 2:
        print('offline_toggle enabled, skipping steps 1-3')
        if not os.path.exists(offline_folder):
            modlog.error('Developer offline_toggle set before downloading files.. EXITING')
            sys.exit()
        report_df = pd.read_csv(f'./{args.local_directory}/offline/REPORT.csv')
        chemdf_dict = {}
        chemdf_dict = {
            'ECL' : pd.read_csv(f'./{args.local_directory}/offline/ECL.csv',
                                index_col='InChI Key (ID)'),
            'HC' : pd.read_csv(f'./{args.local_directory}/offline/HC.csv',
                                index_col='InChI Key (ID)'),
            'LBL' : pd.read_csv(f'./{args.local_directory}/offline/LBL.csv',
                                index_col='InChI Key (ID)'),
            'MIT_PVLab' : pd.read_csv(f'./{args.local_directory}/offline/MIT_PVLab.csv',
                                      index_col='InChI Key (ID)')
        }

    debug_header = f"# Report version {__version__}; Created on {datetime.datetime.now()}; Dataset(s) targeted {dataset_list}\n"
    set_debug_header(debug_header)

    if args.debug:
        # Export dataframes of initial parsing and chemical inventories for ETL to ESCALATEV3
        report_csv_filename = f'REPORT_{target_naming_scheme.upper()}.csv'
        write_debug_file(report_df, report_csv_filename)
        for name, chemicaldf in chemdf_dict.items():
            inventory_name = f'REPORT_{name.upper()}_INVENTORY.csv'
            write_debug_file(chemicaldf, inventory_name)

    compound_ingredient_objects_df = ingredient_pipeline(report_df,
                                                         chemdf_dict,
                                                         args.debug)
    feat_pipeline(target_naming_scheme, report_df, chemdf_dict, args.debug, log_directory)
    sum_molarity_df = calc_pipeline(report_df,
                                    compound_ingredient_objects_df,
                                    chemdf_dict,
                                    args.debug) 

    #augmented_raw_df = augmentdataset(report_df)
    #rxn_v1molarity_clean = nameCleaner(clean_df.filter(like='_raw_v1-M_'), '_rxn_M')
    # TODO: cleanup documentation and export pipeline for statesets
    # TODO: create final export of a 2d CSV file from the data above

    if ('state' in vars(args)):
        templink = str(args.state)
        link = templink.split('.')[0] + '.link.csv'
        pass
    else:
        modlog.error('User MUST specify a stateset during version data repo\
                     upload preparation!')
        sys.exit()

    if args.verdata is not None:
        export_to_repo.prepareexport(target_naming_scheme, args.state, link, args.verdata)
            
    elif args.verdata == 0:
        modlog.info('No versioned data repository format generated')

    elif args.verdata == None:
        modlog.info(f'No versioned data export selected, exiting cleanly, please use the generated {target_naming_scheme}.csv file')
        print(f'Exiting cleanly, please use the generated {target_naming_scheme}.csv file')

    if offline_toggle == 0: 
        os.remove('./mycred.txt') #cleanup automatic authorization

