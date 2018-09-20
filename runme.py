#Copyright (c) 2018 Ian Pendleton - MIT License
#version 0.2
from expworkup import jsontocsv
from expworkup import createjson
import argparse as ap
import os
from expworkup import googleio

version=0.2

# Some command line interfacing to aid in script handling
parser = ap.ArgumentParser(description='Target Folder')
parser.add_argument('folder', type=str, help='Please include target folder') 

## Debug option for future use?
#parser.add_argument('--Debug', type=int, help='Debug=1 prints raw to csv, debug=0 prints only data for learning (default=0)', default=0)

args = parser.parse_args()
myjsonfol = args.folder

#debug = args.Debug

#Ensure directories are in order
if not os.path.exists('data/datafiles'):
    os.mkdir('data/datafiles')
if not os.path.exists(myjsonfol):
    os.mkdir(myjsonfol)

#run the main body of the code.  Can be called later as a module if needed
createjson.ExpDirOps(myjsonfol) #Run Primary JSON Creator
jsontocsv.printfinal(myjsonfol) # RUn the JSON to CSV parser
