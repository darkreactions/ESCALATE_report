import json
import pandas as pd
import argparse as ap
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from operator import itemgetter

parser = ap.ArgumentParser(description='Target Folder')
parser.add_argument('Filename', type=str, help='Please include target folder') 
parser.add_argument('--Debug', type=int, help='Debug=1 prints raw to csv, debug=0 prints only data for learning (default=0)', default=0)
args = parser.parse_args()
myjsonfol = args.Filename
debug = args.Debug
finalvol_entries=2 ## Hard coded number of formic acid entries at the end of the run

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

#Import the most recent chemical data sheet from google drive to process the inchi keys and data about chemicals
#Eventually needs to be linked to database import and broader database information
def ChemicalData():
    print('Obtaining chemical information from Google Drive..', end='')
    chemsheetid = "1JgRKUH_ie87KAXsC-fRYEw_5SepjOgVt7njjQBETxEg"
    ChemicalBook = gc.open_by_key(chemsheetid)
    chemicalsheet = ChemicalBook.get_worksheet(0)
    chemical_list = chemicalsheet.get_all_values()
    chemdf=pd.DataFrame(chemical_list, columns=chemical_list[0])
    chemdf=chemdf.iloc[1:]
    chemdf=chemdf.reset_index(drop=True)
    chemdf=chemdf.set_index(['InChI Key (ID)'])
    print('.done')
    return(chemdf)

#Flattens the list and returns the heirchical naming structure 0 ... 1 ... 2  ## See the example in the faltten_json_reg definition for more details
def flatten_json(y):
    out = {}
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[str('_raw_'+ name[:-1])] = str(x)
    flatten(y)
    return(pd.io.json.json_normalize(out))

#Flattens the list and returns the heirchical naming structure 0 ... 1 ... 2  (this function returns the reagent information in the proper namespace)
def flatten_json_reg(y):
    out = {}
    def flatten(x, name=''):
        #Flattens the dictionary type to a single column and entry
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        #Flattens list type to a single column and entry
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            ## Parses the namespace if the units are contained with the value from the JSON
            if ':' in str(x):
                x1,x2=x.split(":")
                out[str('_raw_reagent_' + name[:-1])] = x1 #Drops to the default namespace 
                out[str('_raw_reagent_' + name[:-1] + '_units')] = str(x2) # adds a new entry for the column with the units which is adjacent to the measurement and otherwise has an identical name
            else:
                out[str('_raw_reagent_' + name[:-1])] = str(x) #If the JSON value was a not a unit based value, the key value pair is returned as a column header and entry
    flatten(y) # Flattens the rest of the formating for pandas import
    return(pd.io.json.json_normalize(out)) # normalizes the data and reads into a single row data frame


#code for breaking out the list of lists and creating the relevant data frame (returns dataframe)
def dict_listoflists(list_lists):
    values=[]
    for item in list_lists:
#        key=str(item[0])
#        key=str('_rxn_'+key[:-1])
#        keys.append(key)
        value=item[1]
        values.append(value)
    tray=pd.DataFrame(values)#, columns=['_rxn_temperatureC', '_rxn_stirrateRPM','_rxn_mixingtime1S','_rxn_mixingtime2S','_rxn_reactiontimeS'])
    tray_df=tray.transpose()
    tray_df.columns =['_rxn_temperatureC', '_rxn_stirrateRPM','_rxn_mixingtime1S','_rxn_mixingtime2S','_rxn_reactiontimeS']
    return(tray_df)

#Will eventually create a dataframe from the robot handling information
def robo_handling():
    pass

def reag_info(reagentdf,chemdf):
    ## ignore GBL ##
    reagentlist=[]
    for item in list(reagentdf):
        name='null'
        mm='null'
        density='null'
        if ('_raw_reagent_' in item) and ('InChIKey' in item):
            for InChIKey in reagentdf[item]:
                m_type='null'
                parse_name=item[:-21]
                parse_chemical_name=item[:-9]
        ### Eventually this section of code can be replaced by class objects which describe the reagent for each experiment in line
        ### Currenlty this code is assembling a dataframe which describes the relatinship between various reagents in order
        ### to perform subsequent calcualtions.  The inchi keys for example are hard coded and should be variable
                #FA
                if InChIKey == "BDAGIHXWWSANSR-UHFFFAOYSA-N":
                    mm=(float(chemdf.loc[InChIKey,"Molecular Weight (g/mol)"]))# / float(chemdf.loc["FAH","Density            (g/mL)"])
                    name=((chemdf.loc[InChIKey,"Chemical Name"]))# / float(chemdf.loc["FAH","Density            (g/mL)"])
                    m_type='pureliquid' #Assigns an object type for later analysis and calculation of concentrations
                    try:
                        density=(float(chemdf.loc[InChIKey,"Density            (g/mL)"]))# / float(chemdf.loc["FAH","Density            (g/mL)"])
                    except:
                        print("Error with the configuration of reference sheet.  Abort run and check formic acid details in google sheets (2018-08-11)")
                        break
                #GBL
                elif InChIKey =='YEJRWHAVMIAJKC-UHFFFAOYSA-N': 
                    mm=(float(chemdf.loc[InChIKey,"Molecular Weight (g/mol)"]))# / float(chemdf.loc["FAH","Density            (g/mL)"])
                    name=((chemdf.loc[InChIKey,"Chemical Name"]))# / float(chemdf.loc["FAH","Density            (g/mL)"])
                    m_type='solvent'
                    try:
                        density=(float(chemdf.loc[InChIKey,"Density            (g/mL)"]))# / float(chemdf.loc["FAH","Density            (g/mL)"])
                    except:
                        pass
                #PbI2
                elif InChIKey == 'RQQRAHKHDFPBMC-UHFFFAOYSA-L':
                    mm=(float(chemdf.loc[InChIKey,"Molecular Weight (g/mol)"]))# / float(chemdf.loc["FAH","Density            (g/mL)"])
                    name=((chemdf.loc[InChIKey,"Chemical Name"]))# / float(chemdf.loc["FAH","Density            (g/mL)"])
                    m_type='inorg'
                #Null
                elif InChIKey == 'null':
                    pass
                #Organic
                else:
                    mm=(float(chemdf.loc[InChIKey,"Molecular Weight (g/mol)"]))# / float(chemdf.loc["FAH","Density            (g/mL)"])
                    name=((chemdf.loc[InChIKey,"Chemical Name"]))# / float(chemdf.loc["FAH","Density            (g/mL)"])
                    m_type='org'
                    try:
                        density=(float(chemdf.loc[InChIKey,"Density            (g/mL)"]))# / float(chemdf.loc["FAH","Density            (g/mL)"])
                    except:
                        pass
                for (item) in list(reagentdf):
                    if (parse_chemical_name in item) and ("actual_amount" in item) and not ("units" in item):
                        amount=(reagentdf.loc[0,item])
            reagentlist.append((name, mm, density, parse_name, InChIKey, m_type, amount))
    reagentlist_df=(pd.DataFrame(reagentlist, columns=['name', 'molecular mass', 'density', 'parsed name', "InChIKey", "m_type", "amount"]))
    return(reagentlist_df)

def ReagentConc(one_reagent_df, reagent):
    df=one_reagent_df
    index=0
    conc={}
    reagent=reagent[5:]
    #Calculateds the concentration of the REAGENTS being used in each experiment (signified largely by a single runID_vial combination)
    for m_type in df['m_type']:
        if m_type == 'null':
            pass
        if m_type == 'solvent':
            solv_mm=float(df.loc[(index,'molecularmass')])
            solv_d=float(df.loc[(index,'density')])
            solv_A=float(df.loc[(index,'amount')])
        index+=1
    index=0
    #calculates the values for the reagent concentrations in terms of the total solvent used in the preparation (for inorganic and organic components)
    for m_type in df['m_type']:
        if (m_type == 'inorg') or (m_type == 'org'):
            mm=float(df.loc[(index,'molecularmass')])
            A=float(df.loc[(index,'amount')])
            calculated_concentration=(A / mm ) / ( solv_A / 1000) ## Returns the value in mmolarity
#            name_space=('_rxn_' + reagent + "_chemical_" + str(index) + "_conc_" + (df.loc[(index,'InChiKey')]))
            name_space=('_raw_' + reagent + "_conc_" + (df.loc[(index,'InChiKey')]))
            conc[name_space]=calculated_concentration
        #Calculates the pure "density" of the compound and uses that in place of the solvated concentration using the amounts from the experiment
        if (m_type == 'pureliquid'):
            mm=float(df.loc[(index,'molecularmass')])
            A=float(df.loc[(index,'amount')])
            d=float(df.loc[(index,'density')])
            calculated_concentration=(d/mm*1000)
            name_space=('_raw_' + reagent + "_conc_" + (df.loc[(index,'InChiKey')]))
            conc[name_space]=calculated_concentration
        index+=1
    if conc == {}:
        return('null')
    else:
        return([conc])
    
#Organizes all of the gathered reagent data by reagent and proceeds to send off for calculating molality                        
def calcConc(reagentdf, reagent_spc):
    reagent_spc.set_index(['parsed name'], inplace=True)
    reagent_list=[]
    conc_df=pd.DataFrame()
    for reag_chem, row in reagent_spc.iterrows():
        if reag_chem not in reagent_list:
            reagent_list.append(reag_chem)
        else:
            pass
    count=0
    for reagent in reagent_list:
        count+=1
        for reag_chem, row in reagent_spc.iterrows():
            if reagent in reag_chem:
                chemical_list=[]
                chemical_list.append(reagent_spc.at[reag_chem, 'name'])
                chemical_list.append(reagent_spc.at[reag_chem, 'InChIKey'])
                chemical_list.append(reagent_spc.at[reag_chem, 'density'])
                chemical_list.append(reagent_spc.at[reag_chem, 'm_type'])
                chemical_list.append(reagent_spc.at[reag_chem, 'molecular mass'])
                chemical_list.append(reagent_spc.at[reag_chem, 'amount'])
        onereagent_df=(pd.DataFrame(chemical_list).transpose())
        onereagent_df.columns = ['name','InChiKey', 'density', 'm_type','molecularmass','amount']
        conc_cells = (ReagentConc(onereagent_df, reagent))
        if conc_cells == 'null':
            pass
        else:
            conc_cells_df=pd.DataFrame(conc_cells)
            conc_df=pd.concat([conc_df,conc_cells_df], axis=1)
    return(conc_df)

##Calls support functions which parse experiments (rows of tempdf)
#def clean_df(JsonParsed_df):
#    # Builds the namespace for further operation, a unique identifier for each experiment
#    runID_df=pd.DataFrame(data=[JsonParsed_df['_raw_jobserial'] + '_' + JsonParsed_df['_raw_vialsite']]).transpose()
#    runID_df.columns=['RunID']
#    # Finds inchi keys associated with organic reagents added to a particular experiment
##    og_df=organic_gather(JsonParsed_df, runID_df)
#    descriptor_df=descriptor_calc_prep(JsonParsed_df)
##    for item in list(tempdf):
##        itemlist.append(item)
##    temp_df=pd.concat([runID_df, og_df], axis=1)
##    cleaned_df=temp_df.reindex(sorted(temp_df.columns), axis=1)s
##    return(og_df)
##    return(og_df)

#parases each index of the json file and returns a normalized data frame with each experiment (well) containing all relevant information
def reagentparse(firstlevel, myjson, chem_df):
    for reg_key,reg_value in firstlevel.items():
        if reg_key == 'reagent':
            reagent_df=flatten_json_reg(reg_value)
            reagent_spec=reag_info(reagent_df,chem_df) #takes all of the infomration from the chem_df (online web information here) and puts it together with the the reagent information
            Conc_df=calcConc(reagent_df, reagent_spec) #takes the information abot the reagents and generates ... 

        if reg_key == 'run':
            run_df=flatten_json(reg_value)
        if reg_key == 'tray_environment':
            tray_df=dict_listoflists(reg_value)
         ##### Currently ommitting this line of data as the information here does not add detail to the data structure ### 
#        if reg_key == 'robot_reagent_handling':
#            robo_df=robo_handling(reg_value)
#                print(item)
        ########### See above note, still should parse later!!! after v1.1 ###  
        ###The following column headers are only approxpriate for workflow 1.1, if the namespace changes these lines will need to be reformmated 
        ### rxn reagent_4 and 5 assume that the input chemicals are formic acid, the volume of which can be learned on directly 
        ### this assumtion might not be valid in all future cases
        if reg_key == 'well_volumes':
            well_volumes_df=pd.DataFrame(reg_value, columns=['_raw_vialsite', '_raw_reagent_0_volume', '_raw_reagent_1_volume', '_raw_reagent_2_volume', '_raw_reagent_3_volume', '_raw_reagent_4_volume', '_raw_reagent_5_volume', '_raw_labwareID'])
        if reg_key == 'crys_file_data':
            crys_file_data_df=pd.DataFrame(reg_value, columns=['_raw_vialsite', '_out_crystalscore'])
    #The following code aligns and normalizes the data frames
    experiment_df=well_volumes_df.merge(crys_file_data_df)
    wellcount=(len(experiment_df.index))-1
    fullrun_df=(run_df.append([run_df]*wellcount,ignore_index=True))
    fullConc_df=(Conc_df.append([Conc_df]*wellcount,ignore_index=True))
    fullreagent_df=(reagent_df.append([reagent_df]*wellcount,ignore_index=True))
    fulltray_df=(tray_df.append([tray_df]*wellcount, ignore_index=True))
    out_df=pd.concat([fullConc_df, experiment_df, fullreagent_df, fullrun_df, fulltray_df], axis=1)
    return(out_df)

#returns well and job id labele darray along with all amine associated inchi keys Temp fix for wkflow1.1
#def descriptor_calc_prep(JsonParsed_df):
#    for item in list(JsonParsed_df):
#        #Looks up all rows containing inchi keys
#        if "InChIKey" in item:
#            temp_df=pd.DataFrame(JsonParsed_df[item])
#            #removes all values for Formic acid, GBL and PBI2 (respectively) from the inchikey assessment
#            temp_df=temp_df.replace({item:{'BDAGIHXWWSANSR-UHFFFAOYSA-N': 'null', 'YEJRWHAVMIAJKC-UHFFFAOYSA-N': 'null', 'RQQRAHKHDFPBMC-UHFFFAOYSA-L': 'null'}})
#    return(temp_df)

def calc_mmol(vol, index, reagent_name, JsonParsed_df):
#    print(vol, index, reagent_name)
    mmol_cell={}
    for header in list(JsonParsed_df):
        if ('conc' in header) and (reagent_name in header):
            mmol_name=('_raw_mmol_' + header[20:])
#            print(((vol*JsonParsed_df.loc[index, header]/1000)), header)
            mmol_cell[mmol_name]=((vol*JsonParsed_df.loc[index, header]/1000))
    #Returns a dictionary with the key set to the _raw_mmol + inchi string taken from the parsed mmol name above. 
    return(mmol_cell)


def volcheck(vol_series, reagent_name, JsonParsed_df,runID_df):
    index=0
    mmol_index_list=[]
    mmol_df_out=pd.DataFrame()
    for volume in vol_series:
            runID=runID_df.loc[index,'RunID_vial']
            #calculates the mmol of the reagent and returns to a list (order is important as the indexes are not maintained through this step)
            #The calculation is done indpendently to better handle different chemicals (and thereby be more flexible moving forward) using the index
            mmol_index_list.append(calc_mmol(volume, index, reagent_name, JsonParsed_df))
            index+=1
    mmol_df_out=pd.DataFrame(mmol_index_list)
    if mmol_df_out.size == 0:
        pass
    else:
        return(mmol_df_out)

#add columns with similar inchi keys and returns an array with unique columns
def combine(portioned_df):
    combined_df=pd.DataFrame()
    reagentlist=[]
    mmol_df_dict={}
    for header in list(portioned_df):
        # prevents trying to sum a single item and instead just adds the series back to a dataframe for later use
        if len(list(portioned_df[header].shape)) == 1:
            header_df_name=header+"_final"
            temp_df=pd.DataFrame(portioned_df[header])
            temp_df.columns=[header_df_name]
            mmol_df_dict[header]=temp_df
        else: 
            if header in reagentlist:
                pass
        #Takes all dataframe columns with name "header" and sums the values together returning the summed list to a header referenced dict
        #Probably a better way to do this, but it works for all dataframes in the test set
            else:
                mmol_value=[]
                for index, experiment in portioned_df[header].iterrows():
                    mmol_list=[]
                    for item in experiment:
                        mmol_list.append(item)
                    mmol_value.append((sum(mmol_list)))
                header_df_name=header+"_final"
                mmol_df_dict[header]=pd.DataFrame(mmol_value, columns=[header_df_name])
                reagentlist.append(header)
    #outputs all of the dataframes created during the mmol summing process and returns with new header for later
    for (k,v) in mmol_df_dict.items():
        combined_df=pd.concat([combined_df, v], axis=1)
    return(combined_df)

#splits of the reagents to analyze the volumes of the important reagents for the current workflows (wkflow1.1 is 2,3,4)
def mmol_breakoff(JsonParsed_df, runID_df):
    out_df = pd.DataFrame()
    reagent_mmol_df=pd.DataFrame()
    for columnname in list(JsonParsed_df):
        if ("_raw_reagent_" in columnname) and ("_volume" in columnname):
            if columnname == "_raw_reagent_0_volume":
                pass
            else:
                reagent_name=columnname[:-6]
                reagent_mmol_df=(volcheck(JsonParsed_df[columnname], reagent_name, JsonParsed_df, runID_df))
            out_df=pd.concat([out_df, reagent_mmol_df], axis=1, sort=False)
    out_combined_df=combine(out_df)  
    return(out_combined_df)

def GrabInchi(rxn_mmol_df, labels_df):
## ignore GBL ##
    #Wierd pythoness required me to break this out into a list...
    label_list=[]
    dump=[]
    for item in labels_df:
        label_list.append(str(item))
    label_list_df=pd.DataFrame(label_list, columns=['RunID_vial'])
    ## and literally rebuild the dataframe.  Like.. email me if you figure out why this was needed
    inchi_list=[]
    index=0
    header_list=[]
    for header in list(rxn_mmol_df):
        header_list.append(header)
    for row_label, row in rxn_mmol_df.iterrows():
        dump.append(row_label)
        row_index=0
        for entry in row:
            if entry==0:
                pass
            else:
                header=header_list[row_index]
                InChIKey=header[10:-6]
                if InChIKey == "BDAGIHXWWSANSR-UHFFFAOYSA-N":
                    pass
                elif InChIKey =='YEJRWHAVMIAJKC-UHFFFAOYSA-N': 
                    pass
                elif InChIKey == 'RQQRAHKHDFPBMC-UHFFFAOYSA-L':
                    pass
                elif InChIKey == 'null':
                    pass
                else:
                    inchi_list.append(InChIKey)
                    index+=1
            row_index+=1
    keylist_df=pd.DataFrame(inchi_list, columns=['_rxn_organic-inchikey'])
    out_df=pd.concat([keylist_df, label_list_df], axis=1)
    out_df.set_index('RunID_vial', inplace=True)
    return(out_df)

def nameCleaner(sub_dirty_df):
    inorganic_list=[]
    organic_df=pd.DataFrame()
    cleaned_M=pd.DataFrame()
    for header in sub_dirty_df.columns:
        #GBl handling -- > Solvent labeled
        if 'YEJRWHAVMIAJKC-UHFFFAOYSA-N' in header:
            print("1")
            pass
        #Acid handling --> Acid labeld --> will need to declare type in the future or something
        elif "BDAGIHXWWSANSR-UHFFFAOYSA-N" in header:
            cleaned_M['_rxn_M_acid']=sub_dirty_df[header]
#            molarity_df['_rxn_M_acid'] = mmol_reagent_df[header] / (calculated_volumes_df['_raw_final_volume']/1000)
        #PBI2 handling --> inorganic label
        elif 'RQQRAHKHDFPBMC-UHFFFAOYSA-L' in header:
            cleaned_M['_rxn_M_inorganic']=sub_dirty_df[header]
#            molarity_df['_rxn_M_inorganic'] = mmol_reagent_df[header] / (calculated_volumes_df['_raw_final_volume']/1000)
        else:
            organic_df[header]=sub_dirty_df[header]
    cleaned_M['_rxn_M_organic']=organic_df.sum(axis=1)
    return(cleaned_M)


#cleans up the name space and the csv output for distribution
def Cleaner(dirty_df):
    rxn_M_clean = nameCleaner(dirty_df.filter(like='_raw_M_'))
    rxn_df=dirty_df.filter(like='_rxn_') 
    feat_df=dirty_df.filter(like='_feat_') 
    out_df=dirty_df.filter(like='_out_') 
    if debug == 1: 
        raw_df=dirty_df.filter(like='_raw_')
        squeaky_clean_df=pd.concat([out_df,rxn_M_clean,rxn_df,feat_df, raw_df], axis=1) 
    else:
        squeaky_clean_df=pd.concat([out_df,rxn_M_clean,rxn_df,feat_df], axis=1) 
    return(squeaky_clean_df)

#hard coded around the notion that the last two entries are formic acid.  This will have to be changed!
def molarity_calc(raw_df, finalvol_entries):
    ##Calculate Reagent volumes
    reagent_list=[]
    for header in raw_df.columns:
        if "volume" in header:
            reagent_list.append(header)
    total_list=reagent_list[:-finalvol_entries]
    df_total_list=raw_df[total_list]
    total_vol_df = df_total_list.sum(axis=1)
    df_final_vols=raw_df[reagent_list]
    final_vol_df = df_final_vols.sum(axis=1)
    calculated_volumes_df=pd.concat([total_vol_df, final_vol_df],axis=1)
    calculated_volumes_df.columns=['_raw_total_volume', '_raw_final_volume']
    ## Calculate molarity (grab reagent mmols and then use the volumes caclualted above to detrmine the "nomial molarity")
    mmol_reagent_list=[]
    for header in raw_df.columns:
        if '_raw_mmol_' in header and 'final' in header:
            mmol_reagent_list.append(header)
    mmol_reagent_df = raw_df[mmol_reagent_list]    
    molarity_df = pd.DataFrame()
    for header in mmol_reagent_df:
        newheader='_raw_M_'+header[10:-6]+'_final'
        molarity_df[newheader] = mmol_reagent_df[header] / (calculated_volumes_df['_raw_final_volume']/1000)
    return(molarity_df)

## Unpack logic
    #most granular data for each row of the final CSV is the well information.
    #Each well will need all associated information of chemicals, run, etc. 
    #Unpack those values first and then copy the generated array to each of the invidual wells
    ### developed enough now that it should be broken up into smaller pieces!
def unpackJSON(myjson_fol):
    chem_df=(ChemicalData())  #Grabs relevant chemical data frame from google sheets (only once no matter how many runs)
    concat_df_final=pd.DataFrame()  
    for file in sorted(os.listdir(myjson_fol)):
        print(file)
        if file.endswith(".json"):
            concat_df=pd.DataFrame()  
            #appends each run to the original dataframe
            myjson=(os.path.join(myjson_fol, file))
            workflow1_json = json.load(open(myjson, 'r'))
            #gathers all information from raw data in the JSON file
            tray_df=reagentparse(workflow1_json, myjson, chem_df) #generates the tray level dataframe for all wells including some calculated features
            concat_df=pd.concat([concat_df,tray_df], ignore_index=True, sort=True)
            #generates a well level unique ID and aligns
            runID_df=pd.DataFrame(data=[concat_df['_raw_jobserial'] + '_' + concat_df['_raw_vialsite']]).transpose()
            runID_df.columns=['RunID_vial']
            #Gets the mmol of each CHEMICAL and returns them summed and uniquely indexed
            mmol_df=mmol_breakoff(tray_df, runID_df)
            #combines all operations into a final dataframe for the entire tray level view with all information
            concat_df=pd.concat([mmol_df, concat_df, runID_df], sort=True, axis=1)
        #Combines the most recent dataframe with the final dataframe which is targeted for export
        concat_df_final=pd.concat([concat_df_final,concat_df], sort=True)
        concat_df_final=concat_df_final.fillna(0)
    final_id=concat_df_final['RunID_vial']
    concat_df_final.set_index('RunID_vial', inplace=True)
    #grabs all of the raw mmol data from the column header and creates a column which uniquely identifies which organic will be needed for the features in the next step
    rxn_mmol_df=concat_df_final.filter(like='_raw_mmol_')
    #Sends off the final mmol list to generate inchi list and  for the chemical features calculations
    OrganicInchi_df=GrabInchi(rxn_mmol_df, final_id)
    #takes all of the volume data from the robot run and reduces it into two total volumes, the total prior to FAH and the total after.  Returns a 3 column array "totalvol and finalvol in title"
    molarity_df=molarity_calc(concat_df_final, finalvol_entries)
    #Combines the new Organic inchi file and the sum volume with the main dataframe
    concat_df_final=pd.concat([OrganicInchi_df, concat_df_final, molarity_df], axis=1, join_axes=[concat_df_final.index])
    #Cleans the file in different ways for post-processing analysis
###    ### Insert concentration calculation here!! 
    #bring in the inchi key based features for a left merge
    with open('perov_desc.csv', 'r') as my_descriptors:
       descriptor_df=pd.read_csv(my_descriptors) 
    dirty_full_df=concat_df_final.merge(descriptor_df, left_on='_rxn_organic-inchikey', right_on='_raw_inchikey', how='inner')
    runID_df_big=pd.DataFrame(data=[dirty_full_df['_raw_jobserial'] + '_' + dirty_full_df['_raw_vialsite']]).transpose()
    runID_df_big.columns=['RunID_vial']
    dirty_full_df=pd.concat([runID_df_big, dirty_full_df], axis=1)
    dirty_full_df.set_index('RunID_vial', inplace=True)
    final_out_df=Cleaner(dirty_full_df)
    return(final_out_df)

def printfinal(myjsonfolder):
    cleaned_df=unpackJSON(myjsonfolder)
    with open('Final.csv', 'w') as outfile:
        print('Complete')
        cleaned_df.to_csv(outfile)

printfinal(myjsonfol)
