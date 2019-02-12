#Copyright (c) 2018 Ian Pendleton - MIT License
import os
import logging
import argparse as ap

from expworkup import jsontocsv
from expworkup import createjson
from expworkup import googleio
from tests import logger
    
#record a detailed and organized set of the variables set by the user
def initialize(args):
    ''' Refreshes working environment - logs initialization
    '''
    modlog = logging.getLogger('report.initialize')
    modlog.info('ensuring directories')
    #run the main body of the code.  Can be called later as a module if needed
    #Ensure directories are in order
    if not os.path.exists('data/datafiles'):
        os.mkdir('data/datafiles')
    if not os.path.exists(args.workdir):
        os.mkdir(args.workdir)
    modlog.info(args)
    modlog.info('directory exist')

if __name__ == "__main__":
    # Some command line interfacing to aid in script handling
    parser = ap.ArgumentParser(description='Target Folder')
    parser.add_argument('workdir', type=str,
        help='Please include target folder') 
    parser.add_argument('-d', '--debug', type=int, default=0,
        help='Turns on testing for implementing new features to the front \
            end of the code, prior to distribution through dataset')
    parser.add_argument('--raw', type=int, default=0,
        help='final dataframe is printed with all raw values included')
    parser.add_argument('-v', '--verdata', type=int, default=0,
        help='generates the output in a form ready for upload to the \
            versioned data repo')

    args = parser.parse_args()
    logger.mylogfunc(args)
    modlog = logging.getLogger('report.main')
    #kick off major sections of the code
    initialize(args)
    createjson.ExpDirOps(args.workdir, args.debug) #Run Primary JSON Creator
    jsontocsv.printfinal(args.workdir, args.debug, args.raw) # RUn the JSON to CSV parser
    if args.verdata==1:
        modlog.info('Exporting data to ###.csv for version data upload')
        print('Exporting data to ###.csv for version data upload')
    elif args.verdata==0:
        modlog.info('No versioned data repository format generated')
    else:
        modlog.error('Unsupported verdata option! Please re-enter the CLI request')