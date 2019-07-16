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
drive=GoogleDrive(gauth)

### General Setup Information ###
##GSpread Authorization information
scope= ['https://www.googleapis.com/auth/spreadsheets.readonly']
credentials = ServiceAccountCredentials.from_json_keyfile_name('expworkup/creds/creds.json', scope) 
gc =gspread.authorize(credentials)

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

###Returns a referenced dictionary of processed files as dictionaries {folder title SD2 ID, Gdrive UID}
def drivedatfold(opdir):
    datadir_list = drive.ListFile({'q': "'%s' in parents and trashed=false" %opdir}).GetList()
    dir_dict=[]
    Crys_dict={}
    Expdata_dict={}
    Robo_dict={}
    print('Downloading data ..', end='',flush=True)
    for f in datadir_list:
        if "Template" in f['title']:
            pass
        elif f['mimeType']=='application/vnd.google-apps.folder': # if folder
            modlog.info('downloaded %s from google drive' %f['title'])
            dir_dict.append(f['title'])
            Exp_file_list =  drive.ListFile({'q': "'%s' in parents and trashed=false" %f['id']}).GetList()
            #Generating a set of dictionaries to easily associate the variable name with with the UID.  Most likely a very general way to do this. 
            #I have hard coded the entry to control what files we are pulling and operating on from the google drive.  Users might upload similar names or 
            #do something I can't think of.  This way we control what is loaded into the JSON
            for f_sub in Exp_file_list:
                if "CrystalScoring" in f_sub['title']\
                        or '_observation_interface' in f_sub['title']:
                    Crys_dict[f['title']] = f_sub['id']
                if "ExpDataEntry" in f_sub['title']\
                        or "preparation_interface" in f_sub['title']:
                    Expdata_dict[f['title']] = f_sub['id']
                if "RobotInput" in f_sub['title']\
                        or "ExperimentSpecification" in f_sub['title']:
                    Robo_dict[f['title']] = f_sub['id']
            print('.', end='', flush=True)
    print(' download complete')
    return(Crys_dict, Robo_dict, Expdata_dict, dir_dict) # Returns a named list of dictionaries linked to the folder (the job jun) and the specific file's UID on gdrive. Each dictionary variable is linked to folder/run
###Returns a referenced dictionary of processed files as dictionaries {folder title SD2 ID, Gdrive UID}, the dictionary labels are thereby callable by the same key, but have different variables.. this makes sense, but likely a better way?

#Converts the hacked google sheets file into a TSV type file  (should eventually store as a json object)
def sheet_to_tsv(expUID, workdir,runname):
    if 'ECL' in runname:
        exp_file = drive.CreateFile({'id': expUID}) 
        exp_file.GetContentFile(workdir+exp_file['title'])
    else:
        ExpDataWorkbook = gc.open_by_key(expUID)
        tsv_ready_lists = ExpDataWorkbook.get_worksheet(1)
        json_in_tsv_list = tsv_ready_lists.get_all_values()
        json_file=workdir+runname+'_ExpDataEntry.json'
        with  open(json_file, 'w') as f:
            for i in json_in_tsv_list:
                print('\t'.join(i), file=f) #+ '\n')

#This function pulls the files to the datafiles directory while also setting the format
#This code should be fed all of the relevant UIDs from dictionary assembler above.  Additional functions should be designed to flag new fields as needed
def getalldata(crysUID, roboUID, expUID, workdir, runname):
    Crys_File = gc.open_by_key(crysUID)
    Crys_file_lists = Crys_File.sheet1.get_all_values()
    Crysout=(Crys_file_lists)
#    exp_file.GetContentFile(workdir+exp_file['title'])
    sheet_to_tsv(expUID, workdir, runname)
    robo_file = drive.CreateFile({'id': roboUID}) 
    robo_file.GetContentFile(workdir+robo_file['title'])
    return(Crysout) #Returns only the list of lists for the crystal file, other files are in xls or need to be processed via text for various reasons