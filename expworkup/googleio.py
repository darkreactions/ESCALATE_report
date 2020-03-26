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
from utils.file_handling import get_experimental_run_lab
from utils import globals

from expworkup.devconfig import cwd

modlog = logging.getLogger('report.googleAPI')

def get_gdrive_auth():
    gauth = GoogleAuth(settings_file='settings.yaml')

    google_cred_file = "./mycred.txt"
    if not os.path.exists(google_cred_file):
        open(google_cred_file, 'w+').close()

    gauth.LoadCredentialsFile(google_cred_file)
    if gauth.credentials is None or gauth.access_token_expired:
        gauth.LocalWebserverAuth()  # Creates local webserver and auto handles authentication.
    else:
        gauth.Authorize()  # Just run because everything is loaded properly
    gauth.SaveCredentialsFile(google_cred_file)
    drive = GoogleDrive(gauth)
    return(drive)

### General Setup Information ###
##GSpread Authorization information
def get_gdrive_client():
    '''
    returns instance of gspread client based on credentials in creds.json file
    different scope than googlio.py in ESCALATE_Capture
    '''
    scope = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    #scope = ['https://spreadsheets.google.com/feeds']
    credentials = ServiceAccountCredentials.from_json_keyfile_name('creds.json', scope)
    gc = gspread.authorize(credentials)
    return gc

def ChemicalData(lab):
    """
    Uses google api to gather the chemical inventory targeted by labsheet 'chemsheetid' in dev config

    Parameters
    ----------
    lab : abbreviation of the lab
        lab is specified as the suffix of a folder and the available
        options are included in the lab_vars of the devconfig.py file

    Returns 
    --------
    chemdf : pandas df of the chemical inventory
    """
    gc = get_gdrive_client()
    chemsheetid = globals.lab_safeget(lab_vars, lab, 'chemsheetid')
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
    drive = get_gdrive_auth()
    if 'ECL' in run_name:
        #TODO: Local Copy of JSON logic for testing / comparison
        prep_file = drive.CreateFile({'id': prep_UID})
        prep_file.GetContentFile(os.path.join(local_data_dir, prep_file['title']))
    else:
        gc = get_gdrive_client()
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

    Parameters
    ----------
    remote_directory : Gdrive UID
        UID of the Gdrive parent directory containing ESCALATE runs to process. ESCALATE 
        runs are the child folders of the parent directory.
    local_directory: local folder for saving downloaded files

    Returns
    -----------

    data_directories : dict {<folder_name> : folder_children uids}
        dict keyed on the child foldernames with values being all of the grandchildren 
        gdrive objects (these objects are dictionaries as well with defined structure)
    lablist : list of all unique labs included in the parent directory
    '''
    drive = get_gdrive_auth()

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
    drive = get_gdrive_auth()
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