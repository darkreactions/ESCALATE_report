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
  "private_key_id": "05516a110e2f053145747c432c8124a218118fca",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDPI4jqjofDw1VA\n1VE0q9adAt7T9Ad8IafQURae/yFXsakkJjIgpQficUTDq78/3OYbcjKPayeUmBUp\nn9jb2XVTjouKrUAeGeXO3rB2gZ8fEMLuLQgz1ELwoZkuAWpzxlcUySakO06DEMkw\nZD0zN7jUQqxqlim7eE1VST3tHiLWbtygdOxwxI3qD0XdzMqeEsTBO0u4W5q7G0Rg\ndds2Af3BMddvwk7O8kyiqLXez1HxDBEQcNm1ZNV+sVl1+QEnrzOUGkJ3UcP/pNCB\nAZEd+4hoIeDAhR2HiLh/jGS55tigcn781QxbDlqfoE5dz/xeJRlDO1GZDDJaeQ7J\nuGhJ27wTAgMBAAECggEAHeF0aNGyyAyvibC8DCsVxISbfFvhkIiSWry31KvdNXdN\nfQd9h7QG1SWd09Q8vIuzLhZlMMc2aHsf4mdKszxFbo5Llu+zJiR6QENjlVTRjXuv\ngwg//KoMFgZZwIc3wgfEnB0AVASyKLoNK8vqAC9znDsaAC41SvPpw/nS0xfb0q7c\n8PZhM9ER3RsnsCeNWDInVkLMl7rF+yLpeVK+zG64TlytdcID77LaPVemW4mCkh+9\nrnaAjzAKHxm+jaRkQw8m6E51p6HW1Flo64Xv969mcqHmDQoqEziT+ey33Hu3Trw8\n70B0s2oeenxxeKMZbhgvQo6xztwe8JimPLxbawY1QQKBgQDtU2jjbSkiTzEp7yPp\nRV996U6B0+59J4939zfZ3VLm/FLtWKcsO6usxTirAxGzd8hVTi4y2WN4N/Wz7Y2p\n9XBhLGM5BgpmIk7uU+zn99gN+I+xrqh4FLm49yxNFV9B2m4QEnuF4yuyHnuk0Ja0\nK75OBGPXpk/jEhu8IElONAN1MQKBgQDfcA0BbUKOsaPebbuGGUvLoAgBqN7oO/M4\nxdg9sxXAJIocAt2RHg8Po8NzyE1LaCR9eQkAR+yIWrh18g3Qahjb19ZJWGFZeQfB\nOTBLadoXi1gb7UzUT5bANairIj1Kj/sgkGlXI3yTQjAXMLVMtreq8qTiRD5mcUOh\nQqDCeeIEgwKBgQCbCVtC/yPZCvzmFRhTooMwYQJtY8KvtfFOgIzW4XPv+7Q84yZK\niiyrcCeF6DpfEIgp2inqBAOsHHqBcVWTSwiAIpwrO1v9vrnrjZ39J/bXoaJVg/EA\niSGOyMIDFUwmXAh8rWZOX8pC0REa6T0aNF1c4BdNYJNdlo3RxxG8adQ8cQKBgQCY\nlbmb9tRUBAXHOSKtkgrL1M6C66LF72LKq3lfsTOyUoGqXV6X4nIgmRI5uFjonQcG\nVKiL85IZD/MWQKWkZT/yqfPhhKR+aIOeNYLAjVntaDBUafpkprFpM3uq2qgGikrR\n0yzM4CQLoFCdFZtJ9yF4cVmeV0JRzRmFP63vATMTJwKBgEYOSJaRix+iUk8br0ks\nMLHR/1jpkAKpdylZveDNlH7hyDaI/49BhUiBVfgZpvmmsVBaCuT3zs6EYZgxaKMT\nsNQ79RpFZ37iPcSNcowWx1fA7chbWOU/KadGwajRwyXAJUxf5sqRplFK9uss/7vR\n2IoNs8hKcf097zP3W+60/Adp\n-----END PRIVATE KEY-----\n",
  "client_email": "uploadtogooglesheets@sd2perovskitedrp.iam.gserviceaccount.com",
  "client_id": "101584110543551066070",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://accounts.google.com/o/oauth2/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/uploadtogooglesheets%40sd2perovskitedrp.iam.gserviceaccount.com"
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



