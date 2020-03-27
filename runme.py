#Copyright (c) 2018 Ian Pendleton - MIT License
import logging
import argparse as ap
import os
import sys

import pandas as pd

from expworkup import jsontocsv
from expworkup import createjson
from expworkup import devconfig
from expworkup import googleio
from versiondata import export_to_repo
from expworkup.entity_tables import reagent_entity
from tests import logger
from utils import globals


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
    parser.add_argument('--raw', type=int, default=1, choices=[0, 1],
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
        exp_dict = createjson.download_experiment_directories(args.local_directory, dataset)
        chem_df_dict = createjson.inventory_assembly(exp_dict, chemdf_dict)
    target_naming_scheme = jsontocsv.printfinal(args.local_directory, args.raw, chem_df_dict, dataset_list)

    if args.verdata is not None:
        export_to_repo.prepareexport(target_naming_scheme, args.state, link, args.verdata)

    if args.export_reagents is True:
        modlog.info(f'Exporting {target_naming_scheme}_models.csv and {target_naming_scheme}_objects.csv')
        versioned_df = export_to_repo.prepareexport(target_naming_scheme, args.state, link, args.verdata)
        reagent_entity.all_unique_ingredients(versioned_df, target_naming_scheme, chem_df_dict, export_observables=True)
            
    elif args.verdata == 0:
        modlog.info('No versioned data repository format generated')

    elif args.verdata == None:
        modlog.info(f'No versioned data export selected, exiting cleanly, please use the generated {target_naming_scheme}.csv file')
        print(f'No versioned data export selected, exiting cleanly, please use the generated {target_naming_scheme}.csv file')
    os.remove('./mycred.txt')
