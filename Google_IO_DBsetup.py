#python
##########################################################
#  _        ___           _                              #
# |_)    o   |   _. ._   |_) _  ._   _| |  _ _|_  _  ._  #
# |_) \/ o  _|_ (_| | |  |  (/_ | | (_| | (/_ |_ (_) | | #
#     /                                                  #
##########################################################

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import json


##Authentication for pydrive, designed globally to minimally generate token (a slow process)
gauth = GoogleAuth()
gauth.LoadCredentialsFile("mycred.txt")
if gauth.credentials is None:
    gauth.LocalWebserverAuth() #Creates local webserver and auto handles authentication.
elif gauth.access_token_expired:
    gauth.LocalWebserverAuth() #Creates local webserver and auto handles authentication.
else:
    gauth.Authorize() #Just run because everything is loaded properly
gauth.SaveCredentialsFile("mycred.txt")
drive=GoogleDrive(gauth)

## Big security no no here... this will need to be fixed! ## 

credsjson={
  "type": "service_account",
  "project_id": "sd2perovskitedrp",
  "private_key_id": "a7592f0e9b6e7716a0414d2397063c2f066eb460",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC2F0uwdyw9TFxo\nATFz9+ykCyApChyrYKwF+9/UvwYujLsHc+87KjqmxjvkiW4aM1t+5geY8ihybZPK\ngQ+ZjVN8h3JpWQkvSzBcr3Um79ezIrrJB/bRyTFF+vetExio1Lt1ZzmyJbl9KWKB\nIp3ZStC/qxmAGdoW/QtwgBqL0VnuGUJWY/KENu1hC0oId0wm657ttjYE4N+lvu31\nyA5yM3mxJw+7+VFEa7HT6S+n4jo+kXNi0DQ8PJuKYDyM2zQ5yIRzGjNQa5rsRazM\n3f/ldUvrKGTUQ6c/qf9wOLs4phlPRZ5x8IE2mgYkOvg9TbVbsC1/8EILqW/jYraj\nLtmTqwy1AgMBAAECggEAC99O9w1+G+0LpVhWkhobGsMC8MoQ/neKH8XBXUyrKYPR\nefXsJi5lrpQEOa1gOWMZ1xdGbYl0a6mLVku96aQWSmtGYWoFuEituY1TFRvUM4DT\nLuJPDHSSut5XIbi6BeAA7PGzCFN/WZLGMmdAZXeETsroGbsVxiPviAFCOdWHiIeO\nrPrQwJQDkfiyCw6h2YRfBrKwN6eRg+P0R2+1+W2nSZ97UyNg8/A4U7qiZm8xcCrv\n2Ir4qDQxMP7bkiPXVq5Iach5UL3m6Wdu3AM+pqciDHfIqUnrpxdyXk4VUq3zUYWb\nzLYuhMI+1ACdki0m758WGKZN5qiI3ziDokJKM57awQKBgQD2tP0UZgbZGnbMHnry\nOVeF+NUYgcJssccH8lV8bDHOQN/C0I4ERAopqvI7WRNKZJhFIauQKmLjvG0nv+AZ\nd2Pzr40qINAJbGcc7uBVJIA2XTRfOofhCTREGLnYKVbrgeQCC1YGB/KaLOp+UQde\nCkauIM7MC9g9gIvL/4+A/WpusQKBgQC88zX/C6EfXDEojjiv845u7L+hS8YhREft\n9U8ja1aj/ji+xf4T08v6WoNwI9EKnC69iHLAwbLP4KOLmsAu6wkEB2CEHE2lEnN8\nMIrHGQohgC8Gy11q96rJE4eJU8VxiIw/lFETiib/RcVyvv+jqIV+MG+cpOfuZtSW\nt6MnGxJnRQKBgE2pxi3gvHEl4pX5VmzgXkwffD3dw23iPSykPgMQMFGknIxAiSSQ\nor5hQSYrsWXu6vyAT/jvTvgwPhCQV6TMr9trvT2w3KzKwl8aV+aVugLjLnR4AYR3\nGEwDmKWSxfkXh8aY+PinEdk1IJCpQ294Pq3cSB118RnTWK7cgblyjUnRAoGBAJIU\nCbvLVt6y3MJ46bSGPKjfWeuudFgFvQJoM62zb4FLqr06vwq/JKTB03ogBp4IT05y\nrhz942s5RddJZakgRpEZzvF0HEcxc50gvjnczutFeZXsJaXsIdpgwdlWrX/vzFXf\nKatMlIeoflUO+v6g68u6UJ+vEixKzbJT+Mvj7x9tAoGAVVmTin77Hc/kseUef67F\n3TIGi6xGEmPOcDaB8+Bmyl0M7KHBn79m4qrNdEbg2aiSsXRNEQqoZzq1yB6MfE2B\n8LBpqxuI1zXBXqDO1YvEVr31oCIdzGh4hq1H5+o/y8IIw9jGdVknD9Tp5AHcGcqf\nn66jpuW0+wKvrclKn2Juhuk=\n-----END PRIVATE KEY-----\n",
  "client_email": "sd2jsonworkflow@sd2perovskitedrp.iam.gserviceaccount.com",
  "client_id": "117462869291207799940",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/sd2jsonworkflow%40sd2perovskitedrp.iam.gserviceaccount.com"
}

### General Setup Information ###
##GSpread Authorization information
scope= ['https://spreadsheets.google.com/feeds']
credentials = ServiceAccountCredentials.from_json_keyfile_dict(credsjson, scope)
gc =gspread.authorize(credentials)

def ChemicalData():
    print('Obtaining chemical information from Google Drive..', end='')
    chemsheetid = "1JgRKUH_ie87KAXsC-fRYEw_5SepjOgVt7njjQBETxEg"
    ChemicalBook = gc.open_by_key(chemsheetid)
    chemicalsheet = ChemicalBook.get_worksheet(0)
    chemical_list = chemicalsheet.get_all_values()
    print('...', end='')
    chemdf=pd.DataFrame(chemical_list, columns=chemical_list[0])
    chemdf=chemdf.iloc[1:]
    chemdf=chemdf.reset_index(drop=True)
    chemdf=chemdf.set_index(['Chemical Abbreviation'])
    print('.done')
    return(chemdf)

###Returns a referenced dictionary of processed files as dictionaries {folder title SD2 ID, Gdrive UID}
def drivedatfold(opdir):
    datadir_list = drive.ListFile({'q': "'%s' in parents and trashed=false" %opdir}).GetList()
    dir_dict=[]
    Crys_dict={}
    Expdata_dict={}
    Robo_dict={}
    for f in datadir_list:
        if "Template" in f['title']:
            pass
        elif f['mimeType']=='application/vnd.google-apps.folder': # if folder
            dir_dict.append(f['title'])
            Exp_file_list =  drive.ListFile({'q': "'%s' in parents and trashed=false" %f['id']}).GetList()
            #Generating a set of dictionaries to easily associate the variable name with with the UID.  Most likely a very general way to do this. 
            #I have hard coded the entry to control what files we are pulling and operating on from the google drive.  Users might upload similar names or 
            #do something I can't think of.  This way we control what is loaded into the JSON
            for f_sub in Exp_file_list:
                if "CrystalScoring" in f_sub['title']:
                    Crys_dict[f['title']]=f_sub['id']
                if "ExpDataEntry" in f_sub['title']:
                    Expdata_dict[f['title']]=f_sub['id']
                if "RobotInput" in f_sub['title']:
                    Robo_dict[f['title']]=f_sub['id']
    return(Crys_dict, Robo_dict, Expdata_dict, dir_dict) # Returns a named list of dictionaries linked to the folder (the job jun) and the specific file's UID on gdrive. Each dictionary variable is linked to folder/run
###Returns a referenced dictionary of processed files as dictionaries {folder title SD2 ID, Gdrive UID}, the dictionary labels are thereby callable by the same key, but have different variables.. this makes sense, but likely a better way?

#Converts the hacked google sheets file into a TSV type file  (should eventually store as a json object)
def sheet_to_tsv(expUID, workdir,runname):
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



