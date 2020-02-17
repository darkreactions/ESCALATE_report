#Copyright (c) 2018 Ian Pendleton - MIT License
import json
import pandas as pd
import logging
import time
import sys
import os

import gspread
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from oauth2client.service_account import ServiceAccountCredentials
from tqdm import tqdm
from expworkup.devconfig import lab_vars
from utils import globals

from expworkup.devconfig import cwd

modlog = logging.getLogger('report.googleAPI')

##Authentication for pydrive, designed globally to minimally generate token (a slow process)

gauth = GoogleAuth()
GoogleAuth.DEFAULT_SETTINGS['client_config_file'] = "./client_secrets.json"
gauth.LoadCredentialsFile("./mycred.txt")
if gauth.credentials is None:
    gauth.LocalWebserverAuth() #Creates local webserver and auto handles authentication.
elif gauth.access_token_expired:
    gauth.LocalWebserverAuth() #Creates local webserver and auto handles authentication.
else:
    gauth.Authorize() #Just run because everything is loaded properly
gauth.SaveCredentialsFile("./mycred.txt")
drive = GoogleDrive(gauth)

### General Setup Information ###
##GSpread Authorization information
scope = ['https://www.googleapis.com/auth/spreadsheets.readonly']
credentials = ServiceAccountCredentials.from_json_keyfile_name('./creds.json', scope) 
gc = gspread.authorize(credentials)

def ChemicalData():
    '''
    Uses google api to gather the chemical inventory targeted by labsheet 'chemsheetid' in dev config
    '''

    chemsheetid = lab_vars[globals.get_lab()]['chemsheetid']
    ChemicalBook = gc.open_by_key(chemsheetid)
    chemicalsheet = ChemicalBook.get_worksheet(0)
    chemical_list = chemicalsheet.get_all_values()
    chemdf=pd.DataFrame(chemical_list, columns=chemical_list[0])
    chemdf=chemdf.iloc[1:]
    chemdf=chemdf.reset_index(drop=True)
    chemdf=chemdf.set_index(['InChI Key (ID)'])
    modlog.info('Successfully loaded chemical data for processing')

    return(chemdf)

def save_prep_interface(prep_UID, local_data_dir, run_name):
    """todo gary can we run on this?

    I'm not really sure what goes on with the ECL data, and the other case is Ian's JSON sheet logic
    """
    if 'ECL' in run_name:
        #TODO: Local Copy of JSON logic for testing / comparison
        prep_file = drive.CreateFile({'id': prep_UID})
        prep_file.GetContentFile(os.path.join(local_data_dir, prep_file['title']))
    else:

        prep_workbook = gc.open_by_key(prep_UID)
        tsv_ready_lists = prep_workbook.get_worksheet(1)
        json_in_tsv_list = tsv_ready_lists.get_all_values()
        json_file = local_data_dir + '/' + run_name + '_ExpDataEntry.json' # todo this doesnt make sense for MIT
        modlog.info(f'Parsing TSV to JSON from gdrive. RunID: {json_file}')
        with open(json_file, 'w') as f:
            for i in json_in_tsv_list:
                print('\t'.join(i), file=f)
        f.close()

def parse_gdrive_folder(remote_directory, local_directory):
    ''' Handle the download and local management of files when interacting with gdrive

    :param remote_directory: UID of the Gdrive directory containing ESCALATE runs to process.
    :return: 
    '''
    data_directories = {}

    print('Retrieving Directory Structure...')
    remote_directory_children = drive.ListFile({'q': "'%s' in parents and trashed=false" % remote_directory}).GetList()
    for child in tqdm(remote_directory_children):
        if child['mimeType'] == 'application/vnd.google-apps.folder':
            modlog.info('downloaded file structure for {} from google drive'.format(child['title']))

            grandchildren = drive.ListFile({'q': "'{}' in parents and trashed=false".format(child['id'])}).GetList()
            data_directories[child['title']] = grandchildren

    return data_directories

def gdrive_download(local_directory, child, grandchildren):
    modlog.info(f"Starting on {child} files, saving to {local_directory}")
    for grandchild in grandchildren:
        #TODO: generalize this
        if "CrystalScoring" in grandchild['title']\
                or '_observation_interface' in grandchild['title']:
            observation_file = drive.CreateFile({'id': grandchild['id']})
            observation_file_title = os.path.join(local_directory, f"{observation_file['title']}.csv")
            observation_file.GetContentFile(observation_file_title,mimetype='text/csv')

        if "ExpDataEntry" in grandchild['title']\
                or "preparation_interface" in grandchild['title']:
            save_prep_interface(grandchild['id'], local_directory, child)

        if "RobotInput" in grandchild['title']\
                or "ExperimentSpecification" in grandchild['title']:
            vol_file = drive.CreateFile({'id': grandchild['id']})
            vol_file.GetContentFile(os.path.join(local_directory, vol_file['title']))
    return 