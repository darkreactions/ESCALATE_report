#Copyright (c) 2018 Ian Pendleton - MIT License
import json
import pandas as pd
import logging
import sys
import os

import gspread
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from oauth2client.service_account import ServiceAccountCredentials
from tqdm import tqdm

from expworkup.devconfig import cwd

modlog = logging.getLogger('report.googleAPI')

##Authentication for pydrive, designed globally to minimally generate token (a slow process)
gauth = GoogleAuth()
GoogleAuth.DEFAULT_SETTINGS['client_config_file'] = "expworkup/creds/client_secrets.json"
gauth.LoadCredentialsFile("%s/expworkup/creds/mycred.txt" %cwd)
if gauth.credentials is None:
    gauth.LocalWebserverAuth() #Creates local webserver and auto handles authentication.
elif gauth.access_token_expired:
    gauth.LocalWebserverAuth() #Creates local webserver and auto handles authentication.
else:
    gauth.Authorize() #Just run because everything is loaded properly
gauth.SaveCredentialsFile("expworkup/creds/mycred.txt")
drive = GoogleDrive(gauth)

### General Setup Information ###
##GSpread Authorization information
scope = ['https://www.googleapis.com/auth/spreadsheets.readonly']
credentials = ServiceAccountCredentials.from_json_keyfile_name('expworkup/creds/creds.json', scope) 
gc = gspread.authorize(credentials)

def ChemicalData():
    chemsheetid = "1JgRKUH_ie87KAXsC-fRYEw_5SepjOgVt7njjQBETxEg"
    ChemicalBook = gc.open_by_key(chemsheetid)
    chemicalsheet = ChemicalBook.get_worksheet(0)
    chemical_list = chemicalsheet.get_all_values()
    chemdf=pd.DataFrame(chemical_list, columns=chemical_list[0])
    chemdf=chemdf.iloc[1:]
    chemdf=chemdf.reset_index(drop=True)
    chemdf=chemdf.set_index(['InChI Key (ID)'])
    modlog.info('Successfully loaded chemical data for processing')
    return(chemdf)


def drivedatfold(remote_directory):
    """Parse a GDrive directory of ESCALATE runs.

    Iterates through remote_directory to obtain all of the CrystalScoring, ExpDataEntry, and RobotInput file UIDs,
    along with UIDs of all subdirectories, which should each be the output of an ESCALATE run

    :param remote_directory: UID of the Gdrive directory containing ESCALATE runs to process.
    :return: (dict, dict, dict, list): CrystalScoring, ExpDataEntry, and RobotInput name => UID dicts and subdir list
    """
    # get all of the child folders of remote directory
    remote_directory_children = drive.ListFile({'q': "'%s' in parents and trashed=false" % remote_directory}).GetList()

    # get all of the CrystalScoring, ExpDataEntry, and RobotInput file UIDs in child directory
    print('Downloading data ..', end='', flush=True)
    data_directories = []
    crystal_files = {}
    exp_data_entry_files = {}
    robot_files = {}
    for child in tqdm(remote_directory_children):

        # if folder
        if child['mimeType'] == 'application/vnd.google-apps.folder':
            modlog.info('downloaded {} from google drive'.format(child['title']))
            data_directories.append(child['title'])

            grandchildren = drive.ListFile({'q': "'{}' in parents and trashed=false".format(child['id'])}).GetList()
            for grandchild in grandchildren:
                # todo generalize this
                if "CrystalScoring" in grandchild['title']:
                    crystal_files[child['title']] = grandchild['id']
                if "ExpDataEntry" in grandchild['title']:
                    exp_data_entry_files[child['title']] = grandchild['id']
                if "RobotInput" in grandchild['title']:
                    robot_files[child['title']] = grandchild['id']

    print(' download complete')
    return crystal_files, robot_files, exp_data_entry_files, data_directories


def save_ExpDataEntry_as_json(expUID, workdir, runname):
    if 'ECL' in runname:
        exp_file = drive.CreateFile({'id': expUID}) 
        exp_file.GetContentFile(workdir+exp_file['title'])
    else:
        ExpDataWorkbook = gc.open_by_key(expUID)
        tsv_ready_lists = ExpDataWorkbook.get_worksheet(1)
        json_in_tsv_list = tsv_ready_lists.get_all_values()
        json_file = workdir+runname + '_ExpDataEntry.json'
        with open(json_file, 'w') as f:
            for i in json_in_tsv_list:
                print('\t'.join(i), file=f) #+ '\n')


def getalldata(crysUID, roboUID, expUID, workdir, runname):
    """This function pulls the files to the datafiles directory while also setting the format
    This code should be fed all of the relevant UIDs from dictionary assembler above.
    Additional functions should be designed to flag new fields as needed

    :param crysUID: UID of crystal
    :param roboUID:
    :param expUID:
    :param workdir:
    :param runname:
    :return:
    """
    crystal_file = gc.open_by_key(crysUID)
    crystal_data = crystal_file.sheet1.get_all_values()
#    exp_file.GetContentFile(workdir+exp_file['title'])
    save_ExpDataEntry_as_json(expUID, workdir, runname)
    robo_file = drive.CreateFile({'id': roboUID}) 
    robo_file.GetContentFile(workdir+robo_file['title'])
    # todo: ian: elaborate this. what goes on here with the robot file?
    # (I can figure out what happens to the experimental entry data.
    # Returns only the list of lists for the crystal file,
    # other files are in xls or need to be processed via text for various reasons
    return crystal_data
