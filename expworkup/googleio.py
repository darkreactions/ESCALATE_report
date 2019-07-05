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


def get_drive_UIDs(remote_directory):
    """Get the UIDs of files and folders of interest in Gdrive

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


def save_ExpDataEntry(exp_UID, local_data_dir, run_name):
    """todo gary can we run on this?
    I'm not really sure what goes on with the ECL data, and the other case is Ian's JSON sheet logic
    """
    if 'ECL' in run_name:
        # todo ian: where do these json files come from?
        exp_file = drive.CreateFile({'id': exp_UID})
        exp_file.GetContentFile(os.path.join(local_data_dir, exp_file['title']))
    else:
        exp_data_workbook = gc.open_by_key(exp_UID)
        tsv_ready_lists = exp_data_workbook.get_worksheet(1)
        json_in_tsv_list = tsv_ready_lists.get_all_values()
        json_file = local_data_dir + run_name + '_ExpDataEntry.json'
        with open(json_file, 'w') as f:
            for i in json_in_tsv_list:
                print('\t'.join(i), file=f)


def download_run_data(crys_UID, robo_UID, exp_UID, local_data_dir, run_name):
    """This function pulls the files to the datafiles directory while also setting the format
    This code should be fed all of the relevant UIDs from dictionary assembler above.
    Additional functions should be designed to flag new fields as needed

    :param crys_UID: UID of crystal
    :param robo_UID:
    :param exp_UID:
    :param local_data_dir:
    :param run_name:
    :return:
    """

    # save crystal file
    crystal_workbook = gc.open_by_key(crys_UID)
    crystal_rows = crystal_workbook.sheet1.get_all_values()
    crystal_df = pd.DataFrame.from_records(crystal_rows[1:], columns=crystal_rows[0])
    crystal_df.to_csv(os.path.join(local_data_dir, "{}.csv".format(crystal_workbook.title)),
                      index=False)

    # save exp file
    save_ExpDataEntry(exp_UID, local_data_dir, run_name)

    # save robot file
    robo_file = drive.CreateFile({'id': robo_UID})
    robo_file.GetContentFile(os.path.join(local_data_dir, robo_file['title']))
    return
