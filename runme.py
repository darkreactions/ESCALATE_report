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
from expworkup.handlers import compound_ingredients
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
                        choices=['LBL', 'HC', 'MIT_PVLab', 'dev', 'LBL_WF3_Iodides'],
                        help="Please specify a supported lab from the options \
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
                        help='generates <XXXX>.perovskitedata.csv output in a form ready for upload to the \
                        versioned data repo ||default = None||')
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

    chem_df=googleio.ChemicalData()  #Grabs relevant chemical data frame from google sheets (only once no matter how many runs)

    createjson.download_experiment_directories(args.local_directory, debug)
    target_naming_scheme = jsontocsv.printfinal(args.local_directory, debug, args.raw, chem_df)

    target_naming_scheme = 'perovksitedata_20200117' # testing 
    if args.verdata is not None:
        export_to_repo.prepareexport(target_naming_scheme, args.state, link, args.verdata)

    if args.export_reagents is True:
        modlog.info(f'Exporting {target_naming_scheme}_models.csv and {target_naming_scheme.split}_objects.csv')
        versioned_df = export_to_repo.prepareexport(target_naming_scheme, args.state, link, args.verdata)
        compound_ingredients.all_unique_ingredients(versioned_df, target_naming_scheme, chem_df)
            
    elif args.verdata == 0:
        modlog.info('No versioned data repository format generated')

    else:
        modlog.error('Unsupported verdata option! Please re-enter the CLI\
                     request')
