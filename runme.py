#Copyright (c) 2018 Ian Pendleton - MIT License
import logging
import argparse as ap
import os
import sys
import datetime

import pandas as pd

from expworkup.jsonparser import json_pipeline
from expworkup.createjson import download_experiment_directories
from expworkup.createjson import inventory_assembly
from expworkup.report_calcs import calc_pipeline
from expworkup.report_feats import feat_pipeline
from expworkup import devconfig
from expworkup import googleio
from versiondata import export_to_repo
from expworkup.entity_tables import reagent_entity
from tests import logger
from utils import globals

__version__ = 0.86 #should match latest HISTORY.md entry

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
                              ||default = LBL||",
                        default='LBL'
                        )
    parser.add_argument('--raw', type=bool, default=False, choices=[True, False],
                        help='final dataframe is printed with all raw values\
                        included ||default = 1||')
    parser.add_argument('--export_reagents', type=bool, default=False, choices=[True, False],
                        help='generates <target folder>_models.csv and <target_folder>_objects.csv\
                        corresponding to all compound ingredients in the target dataset (only works for wf11.1) ||default = False||')
    parser.add_argument('--verdata', type=str, 
                        help='Enter numerical value such as "0001". Generates <0001>.perovskitedata.csv output\
                        in a form ready for upload to the versioned data repo ||default = None||')
    parser.add_argument('--state', type=str,
                        help='title of state set file to be used at the state set for \
                        this iteration of the challenge problem, no entry will result in no processing')
    parser.add_argument('--debug', type=bool, default=False, choices=[True, False],
                        help='exports all dataframes as csvfiles with default names')

    args = parser.parse_args()

    logger.mylogfunc(args)
    modlog = logging.getLogger('report.main')

    if ('state' in vars(args)):
        templink = str(args.state)
        link = templink.split('.')[0] + '.link.csv'
        pass
    else:
        modlog.error('User MUST specify a stateset during version data repo\
                     upload preparation!')
        sys.exit()

    dataset_list = args.d
    modlog.info(f'{dataset_list} selected as the dataset target(s) for this run')
    print(f'{dataset_list} selected as the dataset target(s) for this run')
    print(f'{len(dataset_list)} set(s) of downloads will occur, one per dataset, please be patient!')
    modlog.info(f'{len(dataset_list)} set(s) of downloads will occur, one for dataset, please be patient!')

    initialize(args)
    chemdf_dict = {}
    for dataset in dataset_list:
        exp_dict = download_experiment_directories(args.local_directory, dataset)
        chemdf_dict = inventory_assembly(exp_dict, chemdf_dict)

    target_naming_scheme, report_df = json_pipeline(args.local_directory,
                                                    args.raw,
                                                    chemdf_dict,
                                                    dataset_list)

    calc_df = calc_pipeline(target_naming_scheme, report_df, chemdf_dict)
    #feat_df = feat_pipeline(target_naming_scheme, report_df, calc_df, chemdf_dict)


    if args.debug is True:
        
        report_csv_filename = target_naming_scheme+'_debug.csv'
        if os.path.isfile(report_csv_filename):
            os.remove(report_csv_filename)
        f = open(report_csv_filename, 'a')
        f.write(f"# Generated using report version {__version__} on {datetime.datetime.now()} targeting dataset(s) {dataset_list}\n")
        report_df.to_csv(f)
        f.close()

        #calc_df.to_csv(f'{target_naming_scheme}_calcs.csv')
        #calc_df.to_csv(f'{target_naming_scheme}_feats.csv')

        #with open(finaloutcsv_filename, 'w') as outfile:
        #    cleaned_augmented_raw_df.to_csv(outfile)
        #    print(f'{finaloutcsv_filename} rendered successfully')
        #    outfile.close()

    if args.verdata is not None:
        export_to_repo.prepareexport(target_naming_scheme, args.state, link, args.verdata)

    if args.export_reagents is True:
        modlog.info(f'Exporting {target_naming_scheme}_models.csv and {target_naming_scheme}_objects.csv')
        versioned_df = export_to_repo.prepareexport(target_naming_scheme, args.state, link, args.verdata)
        reagent_entity.all_unique_ingredients(versioned_df, target_naming_scheme, chemdf_dict, export_observables=True)
            
    elif args.verdata == 0:
        modlog.info('No versioned data repository format generated')

    elif args.verdata == None:
        modlog.info(f'No versioned data export selected, exiting cleanly, please use the generated {target_naming_scheme}.csv file')
        print(f'No versioned data export selected, exiting cleanly, please use the generated {target_naming_scheme}.csv file')
    #os.remove('./mycred.txt')
