#Copyright (c) 2018 Ian Pendleton - MIT License
import logging
import argparse as ap
import os


from expworkup import jsontocsv
from expworkup import createjson
from expworkup import googleio
from versiondata import export_to_repo
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
                        choices=['LBL', 'HC', 'MIT_PVLab', 'dev'],
                        help="Please specify a supported lab from the options \
                              listed. Selecting 'dev' will change the \
                              directory target to the debugging folder. \
                              Selecting any other lab will target that labs\
                              folders which are specified in devconfig.py\
                              ||default = LBL||",
                        default='LBL'
                        )
#    parser.add_argument('-d', '--debug', type=int, default=0,
#        help='Turns on testing for implementing new features to the front \
#            end of the code, prior to distribution through dataset')
    parser.add_argument('--raw', type=int, default=0,
                        help='final dataframe is printed with all raw values\
                        included ||default = 0||')
    parser.add_argument('-v', '--verdata', type=int, default=0,
                        help='generates the output in a form ready for upload to the \
                        versioned data repo ||default = 0||')
    parser.add_argument('-s', '--state', type=str,
                        help='title of state set file to be used at the state set for \
                        this iteration of the challenge problem')

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
    createjson.ExpDirOps(args.workdir, debug)
    finalcsv = jsontocsv.printfinal(args.local_directory, debug, args.raw)
    if args.verdata == 1:
        export_to_repo.prepareexport(finalcsv, args.state, link)
    elif args.verdata == 0:
        modlog.info('No versioned data repository format generated')
    else:
        modlog.error('Unsupported verdata option! Please re-enter the CLI\
                     request')
