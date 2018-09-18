#Copyright (c) 2018 Ian Pendleton - MIT License
from expworkup import jsontocsv
from expworkup import createjson
import argparse as ap
from expworkup import googleio

parser = ap.ArgumentParser(description='Target Folder')
parser.add_argument('Filename', type=str, help='Please include target folder') 
#parser.add_argument('--Debug', type=int, help='Debug=1 prints raw to csv, debug=0 prints only data for learning (default=0)', default=0)
args = parser.parse_args()
myjsonfol = args.Filename
#debug = args.Debug
finalvol_entries=2 ## Hard coded number of formic acid entries at the end of the run

createjson.ExpDirOps()
jsontocsv.printfinal(myjsonfol)