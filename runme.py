#Copyright (c) 2018 Ian Pendleton - MIT License
import logging
import argparse as ap
import os
import sys

import pandas as pd

from expworkup import jsontocsv
from expworkup import createjson
from expworkup import googleio
from versiondata import export_to_repo
from expworkup.entity_tables import reagent_entity
from tests import logger
from utils import globals


def initialize(args):
    ''' Refreshes working environment - logs initialization

    '''
    modlog = logging.getLogger('report.initialize')
    modlog.info('ensuring directories')
    if not os.path.exists('data/datafiles'):
        os.mkdir('data/datafiles')
    if not os.path.exists(args.local_directory):
        os.mkdir(args.local_directory)
    modlog.info(args)
    modlog.info('directory exist')

if __name__ == "__main__":
    ''' Initialize the main body of the code

    '''
    parser = ap.ArgumentParser(description='Target Folder')
    parser.add_argument('local_directory', type=str,
                        help='Please include target folder')
    parser.add_argument('-l', '--lab',
                        type=str,
                        choices=['LBL', 'HC', 'MIT_PVLab', 'dev',\
                        '4-Data-WF3_Iodide', '4-Data-WF3_Alloying', '4-Data-Bromides'],
                        help="Please specify a supported lab/dataset from the options \
                              listed. Selecting 'dev' will change the \
                              directory target to the debugging folder. \
                              Selecting any other lab will target that labs\
                              folders which are specified in devconfig.py\
                              ||default = LBL||",
                        default='LBL'
                        )
    parser.add_argument('--raw', type=int, default=1, choices=[0, 1],
                        help='final dataframe is printed with all raw values\
                        included ||default = 1||')
    parser.add_argument('-e', '--export_reagents', type=bool, default=False, choices=[True, False],
                        help='generates <target folder>_models.csv and <target_folder>_objects.csv\
                        corresponding to all compound ingredients in the target dataset ||default = False||')
    parser.add_argument('-v', '--verdata', type=str, 
                        help='Enter numerical value such as "0001". Generates <0001>.perovskitedata.csv output\
                        in a form ready for upload to the versioned data repo ||default = None||')
    parser.add_argument('-s', '--state', type=str,
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

    globals.set_lab(args.lab)
    modlog.info('%s selected as the laboratory for this run' % globals.get_lab())
    print('%s selected as the laboratory for this run' % globals.get_lab())
    if globals.get_lab() == 'dev':
        debug = 1
    else:
        debug = 0

    initialize(args)

##### FOR SOME OFFLINE SUPPORT, REQUIRES ONE RUN BEFORE OFFLINE #### 
##### Follow the two step instructions to run post parsing code offline ####
    chem_df=googleio.ChemicalData()                     # 2) Comment out this line
#    chem_df.to_csv('chemdf.csv')                       # 1) Uncomment and run full_report code once
#    chem_df = pd.read_csv('chemdf.csv')                # 2) Uncomment
#    target_naming_scheme = 'perovskitesdata_20191209b' # 2) Uncomment and update to the generated dataset

    createjson.download_experiment_directories(args.local_directory, debug)
    target_naming_scheme = jsontocsv.printfinal(args.local_directory, debug, args.raw, chem_df)

    if args.verdata is not None:
        export_to_repo.prepareexport(target_naming_scheme, args.state, link, args.verdata)

    if args.export_reagents is True:
        modlog.info(f'Exporting {target_naming_scheme}_models.csv and {target_naming_scheme}_objects.csv')
        versioned_df = export_to_repo.prepareexport(target_naming_scheme, args.state, link, args.verdata)
        reagent_entity.all_unique_ingredients(versioned_df, target_naming_scheme, chem_df, export_observables=True)
            
    elif args.verdata == 0:
        modlog.info('No versioned data repository format generated')

    elif args.verdata == None:
        modlog.info(f'No versioned data export selected, exiting cleanly, please use the generated {target_naming_scheme}.csv file')
        print(f'No versioned data export selected, exiting cleanly, please use the generated {target_naming_scheme}.csv file')
