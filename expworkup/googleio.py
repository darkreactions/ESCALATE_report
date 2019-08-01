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
from expworkup.devconfig import lab_vars
from utils import globals
from utils.file_handling import get_interface_filename

from expworkup.devconfig import cwd, valid_input_files

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
    print('Retrieving relevant file pointers...')
    data_directories = []
    spec_interface_files = {}
    observation_interface_files = {}
    prep_interface_files = {}
    pipette_volume_files = {}
    for child in tqdm(remote_directory_children):

        # if folder
        if child['mimeType'] == 'application/vnd.google-apps.folder':
            modlog.info('downloaded {} from google drive'.format(child['title']))
            data_directories.append(child['title'])

            grandchildren = drive.ListFile({'q': "'{}' in parents and trashed=false".format(child['id'])}).GetList()
            for grandchild in grandchildren:
                # todo (dont) generalize this
                if "CrystalScoring" in grandchild['title'] or '_observation_interface' in grandchild['title']:
                    observation_interface_files[child['title']] = grandchild['id']
                if "ExpDataEntry" in grandchild['title'] or "preparation_interface" in grandchild['title']:
                    prep_interface_files[child['title']] = grandchild['id']
                if "RobotInput" in grandchild['title'] or "ExperimentSpecification" in grandchild['title']:
                    pipette_volume_files[child['title']] = grandchild['id']

                # oohwee this could be recursive couldnt it?
                if grandchild['mimeType'] == 'application/vnd.google-apps.folder':
                    greatgrandchildren = drive.ListFile({'q': "'{}' in parents and trashed=false".format(grandchild['id'])}).GetList()
                    for greatgrandchild in greatgrandchildren:

                        # we only need this for custom actions (for now) which will only be in SpecificationInterface
                        if 'SpecificationInterface' in greatgrandchild['title']:
                            spec_interface_files[child['title']] = greatgrandchild['id']

    return observation_interface_files, pipette_volume_files, prep_interface_files, spec_interface_files, data_directories


def save_prep_interface(prep_UID, local_data_dir, run_name):
    """todo gary can we run on this?
    I'm not really sure what goes on with the ECL data, and the other case is Ian's JSON sheet logic
    """
    if 'ECL' in run_name:
        # todo ian: where do these json files come from?
        prep_file = drive.CreateFile({'id': prep_UID})
        prep_file.GetContentFile(os.path.join(local_data_dir, prep_file['title']))
    else:
        prep_workbook = gc.open_by_key(prep_UID)
        tsv_ready_lists = prep_workbook.get_worksheet(1)
        json_in_tsv_list = tsv_ready_lists.get_all_values()
        json_file = local_data_dir + run_name + '_ExpDataEntry.json' # todo this doesnt make sense for MIT
        with open(json_file, 'w') as f:
            for i in json_in_tsv_list:
                print('\t'.join(i), file=f)


def download_run_data(obs_UID, vol_UID, prep_UID, specUID, local_data_dir, run_name):
    """This function pulls the files to the datafiles directory while also setting the format
    This code should be fed all of the relevant UIDs from dictionary assembler above.
    Additional functions should be designed to flag new fields as needed

    :param obs_UID: UID of crystal
    :param vol_UID:
    :param prep_UID:
    :param local_data_dir:
    :param run_name:
    :return:
    """

    # save crystal file
    obs_workbook = gc.open_by_key(obs_UID)
    obs_rows = obs_workbook.sheet1.get_all_values()
    obs_df = pd.DataFrame.from_records(obs_rows[1:], columns=obs_rows[0])
    obs_df.to_csv(os.path.join(local_data_dir, "{}.csv".format(obs_workbook.title)),
                  index=False)

    # save exp file
    save_prep_interface(prep_UID, local_data_dir, run_name)

    # save spec file
    if specUID:
        spec_file = drive.CreateFile({'id': specUID})
        spec_file.GetContentFile(os.path.join(local_data_dir, spec_file['title']))

    # save robot file
    vol_file = drive.CreateFile({'id': vol_UID})
    vol_file.GetContentFile(os.path.join(local_data_dir, vol_file['title']))
    return
