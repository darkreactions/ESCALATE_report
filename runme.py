#Copyright (c) 2018 Ian Pendleton - MIT License
import logging
import argparse as ap
import os
import sys
import datetime

import pandas as pd
import numpy as np


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
    modlog = logging.getLogger('report.initialize')
    modlog.info(args)
    modlog.info('directory exist')

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
    logger.setup_logger('report', main_logger)
    logger.setup_logger('warning', main_logger, level=logging.WARN, stream=True)
    logger.setup_logger('ingredient', ingredient_logger)
    modlog = logging.getLogger('report')
    warnlog = logging.getLogger('warning')
    ingredlog = logging.getLogger('ingredient')

    dataset_list = args.d
    target_naming_scheme = args.local_directory

    # Initial reporting signaling successful code kickoff
    modlog.info(f'{dataset_list} selected as the dataset target(s) for this run')
    print(f'{dataset_list} selected as the dataset target(s) for this run')
    print(f'{len(dataset_list)} set(s) of downloads will occur, one per dataset, please be patient!')
    modlog.info(f'{len(dataset_list)} set(s) of downloads will occur, one for dataset, please be patient!')

    initialize(args)

    # A dev toggle to bypass google downloads after a local iteration
    offline_toggle = 1
    # First iteration, set to '1' to save files locally
    # Second iteration, set to '2' to load local files and continue    
    offline_folder = f'./{args.local_directory}/offline'
    if offline_toggle == 1 or offline_toggle == 0:
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
            modlog.info(f'Ensuring {offline_folder} exists')
            if not os.path.exists(offline_folder):
                os.mkdir(offline_folder)
            report_df.to_csv(f'{offline_folder}/REPORT.csv')
            for name, chemicaldf in chemdf_dict.items():
                chemicaldf.to_csv(f'{offline_folder}/{name}.csv')
    if offline_toggle == 2:
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

    if args.debug is True:
        report_csv_filename = target_naming_scheme+'_debug.csv'
        if os.path.isfile(report_csv_filename):
            os.remove(report_csv_filename)
        f = open(report_csv_filename, 'a')
        f.write(f"# Generated using report version {__version__} on {datetime.datetime.now()} targeting dataset(s) {dataset_list}\n")
        report_df.to_csv(f)
        f.write(f"# Generated using report version {__version__} on {datetime.datetime.now()} targeting dataset(s) {dataset_list}\n")
        f.close()

    compound_ingredient_objects_df = ingredient_pipeline(report_df, chemdf_dict)
    calc_pipeline(report_df, compound_ingredient_objects_df, target_naming_scheme, args.debug) 
    feat_pipeline(target_naming_scheme, report_df, chemdf_dict, args.debug, log_directory)

        #modlog.info(f'Exporting {target_naming_scheme}_models.csv and {target_naming_scheme}_objects.csv')
        #versioned_df = export_to_repo.prepareexport(target_naming_scheme, args.state, link, args.verdata)
        #if nominal is False:
        #    out_name = f'{target_naming_scheme}_objects.csv'

        #elif nominal is True:
        #    out_name = f'{target_naming_scheme}_nominals.csv'
    
        #calc_df.to_csv(f'{target_naming_scheme}_calcs.csv')
        #calc_df.to_csv(f'{target_naming_scheme}_feats.csv')

        #with open(finaloutcsv_filename, 'w') as outfile:
        #    cleaned_augmented_raw_df.to_csv(outfile)
        #    print(f'{finaloutcsv_filename} rendered successfully')
        #    outfile.close()

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
        print(f'No versioned data export selected, exiting cleanly, please use the generated {target_naming_scheme}.csv file')
    #os.remove('./mycred.txt')

