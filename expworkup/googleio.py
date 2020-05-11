#Copyright (c) 2018 Ian Pendleton - MIT License
import json
import pandas as pd
import logging
import time
import sys
import os

import gspread
from gspread.exceptions import APIError
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from oauth2client.service_account import ServiceAccountCredentials
from tqdm import tqdm
from expworkup.devconfig import lab_vars
from utils.file_handling import get_experimental_run_lab
from utils import globals

from expworkup.devconfig import cwd

modlog = logging.getLogger(f'mainlog.{__name__}')
warnlog = logging.getLogger(f'warning.{__name__}')

# Disable super spam from api code
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)

def get_gdrive_auth():
    '''returns instance of the gdrive authentication

    Requires client_secrets.json to generate see:
    https://github.com/darkreactions/ESCALATE_Capture/wiki/Developers:--ONBOARDING-LABS:--Capture-and-Report
    '''
    gauth = GoogleAuth(settings_file='./expworkup/settings.yaml')

    google_cred_file = "./mycred.txt"
    if not os.path.exists(google_cred_file):
        modlog.info(f'Temp authentication file {google_cred_file} created')
        open(google_cred_file, 'w+').close()

    gauth.LoadCredentialsFile(google_cred_file)
    if gauth.credentials is None or gauth.access_token_expired:
        modlog.info(f'Temp authentication file {google_cred_file} required refresh')
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

    requires creds.json to generate see:
    https://github.com/darkreactions/ESCALATE_Capture/wiki/Developers:--ONBOARDING-LABS:--Capture-and-Report
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
    sleep_timer = 0
    chemdf = 0 #just create a fake instance
    while not isinstance(chemdf, pd.DataFrame):
        try:
            gc = get_gdrive_client()
            chemsheetid = globals.lab_safeget(lab_vars, lab, 'chemsheetid')
            ChemicalBook = gc.open_by_key(chemsheetid)
            chemicalsheet = ChemicalBook.get_worksheet(0)
            chemical_list = chemicalsheet.get_all_values()
            chemdf=pd.DataFrame(chemical_list, columns=chemical_list[0])
            chemdf=chemdf.iloc[1:]
            chemdf=chemdf.reset_index(drop=True)
            chemdf=chemdf.set_index(['InChI Key (ID)'])
        except APIError as e:
            modlog.info(e.response)
            modlog.info(sys.exc_info())
            modlog.info('During download of {} chemical inventory sever request limit was met'.format(lab))
            sleep_timer = 15.0
            time.sleep(sleep_timer)
    modlog.info(f'Successfully loaded the chemical inventory from {lab}')
    return(chemdf)

def save_prep_interface(prep_UID, local_data_dir, experiment_name):
    """ Download the hardcoded TSV backend of the preparation interface
    
    Parameters
    ---------
    prep_UID : gdrive UID of the target prep interface

    local_data_dir : folder to store gdrive files
        report default: {target_naming_scheme}/gdrive_files/

    experiment_name :  name of gdrive folder containing the experiment

    Notes
    -----
    ECL data is stored in a JSON file rendered at ECL, all other labs
    are parsed from the TSV backend of the interface.  Example here:
    https://drive.google.com/open?id=1kVVbijwRO_kFeXO74vtIgpEyLenha4ek7n35yp6vxvY
    """
    drive = get_gdrive_auth()
    if 'ECL' in experiment_name:
        #TODO: Local Copy of JSON logic for testing / comparison
        prep_file = drive.CreateFile({'id': prep_UID})
        prep_file.GetContentFile(os.path.join(local_data_dir, prep_file['title']))
    else:
        gc = get_gdrive_client()
        prep_workbook = gc.open_by_key(prep_UID)
        tsv_ready_lists = prep_workbook.get_worksheet(1)
        json_in_tsv_list = tsv_ready_lists.get_all_values()
        json_file = local_data_dir + '/' + experiment_name + '_ExpDataEntry.json' # todo this doesnt make sense for MIT
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

   local_directory : folder to store gdrive files
        report default: {target_naming_scheme}/gdrive_files/

    Returns
    -----------
    data_directories : dict {<folder_name> : folder_children uids}
        dict keyed on the child foldernames with values being all of the grandchildren 
        gdrive objects (these objects are dictionaries as well with defined structure)

    '''
    drive = get_gdrive_auth()

    data_directories = {}
    print('(1/6) Retrieving Directory Structure...')
    remote_directory_children = drive.ListFile({'q': "'%s' in parents and trashed=false" % remote_directory}).GetList()
    for child in tqdm(remote_directory_children):
        if child['mimeType'] == 'application/vnd.google-apps.folder':
            modlog.info('downloaded file structure for {} from google drive'.format(child['title']))
            grandchildren = \
                drive.ListFile({'q': "'{}' in parents and trashed=false".format(child['id'])}).GetList()
            data_directories[child['title']] = grandchildren

    return data_directories

def gdrive_download(local_directory, experiment_name, experiment_files):
    """ Download specific, hard coded files locally
        Only grabs observation interface, preparation interface, and
        experiment specification interfaces (v1.0).  Additional will need to be
        manually added. Parsing of the files happens upstream
    
    Parameters
    ----------
    local_directory : folder to store gdrive files
        report default: {target_naming_scheme}/gdrive_files/
    
    experiment_name :  name of gdrive folder containing the experiment

    experiment_files : list of gdrive objects (files) in experiment_name
        my_file (object in experiment_files) is a gdrive object
        (these objects are dictionaries as with defined structure)

    Returns
    -------
    None

    Notes
    -----
    Though this function returns no objects, the files are in 
    a local directory for manipulation.  Due to the structure of the files
    as well as how finicky google drive API can be, downloading locally 
    was determined to be the best decision.  
    The downloaded files are assessed later. 

    TODO: generalize/broaden this function to grab additional files
    Trick in this case will be that not all runs will have all files
    """

    drive = get_gdrive_auth()
    modlog.info(f"Starting on {experiment_name} files, saving to {local_directory}")
    for my_file in experiment_files:
        if "CrystalScoring" in my_file['title']\
                or '_observation_interface' in my_file['title']:
            observation_file = drive.CreateFile({'id': my_file['id']})
            observation_file_title = os.path.join(local_directory, f"{observation_file['title']}.csv")
            observation_file.GetContentFile(observation_file_title,mimetype='text/csv')

        if "ExpDataEntry" in my_file['title']\
                or "preparation_interface" in my_file['title']:
            save_prep_interface(my_file['id'], local_directory, experiment_name)

        if "RobotInput" in my_file['title']\
                or "ExperimentSpecification" in my_file['title']:
            vol_file = drive.CreateFile({'id': my_file['id']})
            vol_file.GetContentFile(os.path.join(local_directory, vol_file['title']))
    return 