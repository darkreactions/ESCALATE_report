#Copyright (c) 2018 Ian Pendleton - MIT License
import json
import pandas as pd
import os
from operator import itemgetter
import logging

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from tests import logger
from expworkup import googleio
from expworkup.handlers import parser
from expworkup.handlers import calcmmol
from expworkup.handlers import calcmolarity
from expworkup.handlers import inchigen

debug = 0 #args.Debug
finalvol_entries=2 ## Hard coded number of formic acid entries at the end of the run (this needs fixing)

modlog = logging.getLogger('report.JSONtoCSV')

#Will eventually create a dataframe from the robot handling information
def robo_handling():
    pass

#The name cleaner is hard coded at the moment for the chemicals we are using. This will need to be generalized somehow...
def nameCleaner(sub_dirty_df, new_prefix):
    organic_df=pd.DataFrame()
    cleaned_M=pd.DataFrame()
    for header in sub_dirty_df.columns:
        #GBl handling -- > Solvent labeled (or other solvent such as DMF)
        if 'YEJRWHAVMIAJKC-UHFFFAOYSA-N' in header:# or 'ZMXDDKWLCZADIW-UHFFFAOYSA-N' in header:
            pass
        #Acid handling --> Acid labeld --> will need to declare type in the future or something
        elif "BDAGIHXWWSANSR-UHFFFAOYSA-N" in header:
            cleaned_M['%s_acid'%new_prefix]=sub_dirty_df[header]
#            molarity_df['_rxn_M_acid'] = mmol_reagent_df[header] / (calculated_volumes_df['_raw_final_volume']/1000)
        #PBI2 handling --> inorganic label
        elif 'RQQRAHKHDFPBMC-UHFFFAOYSA-L' in header:# or 'ZASWJUOMEGBQCQ-UHFFFAOYSA-L' in header:
            cleaned_M['%s_inorganic'%new_prefix]=sub_dirty_df[header]
#            molarity_df['_rxn_M_inorganic'] = mmol_reagent_df[header] / (calculated_volumes_df['_raw_final_volume']/1000)
        else:
            organic_df[header]=sub_dirty_df[header]
    cleaned_M['%s_organic'%new_prefix]=organic_df.sum(axis=1)
    return(cleaned_M)

#cleans up the name space and the csv output for distribution
def cleaner(dirty_df, raw):
    rxn_molarity_clean = nameCleaner(dirty_df.filter(like='_raw_M_'), '_rxn_M')
    rxn_v1molarity_clean = nameCleaner(dirty_df.filter(like='_raw_v1-M_'), '_rxn_v1-M')
    rxn_df=dirty_df.filter(like='_rxn_')
    feat_df=dirty_df.filter(like='_feat_') 
    out_df=dirty_df.filter(like='_out_') 
    proto_df = dirty_df.filter(like='_prototype_')
    if raw == 1:
        raw_df = dirty_df.filter(like='_raw_')
        squeaky_clean_df = pd.concat([out_df, rxn_molarity_clean,
                                      rxn_v1molarity_clean, rxn_df,
                                      feat_df, raw_df,
                                      proto_df], axis=1)
    else:
        squeaky_clean_df = pd.concat([out_df, rxn_molarity_clean,
                                      rxn_v1molarity_clean, rxn_df,
                                      feat_df, proto_df], axis=1)
    return(squeaky_clean_df)

## Unpack logic
    #most granular data for each row of the final CSV is the well information.
    #Each well will need all associated information of chemicals, run, etc. 
    #Unpack those values first and then copy the generated array to each of the invidual wells
    ### developed enough now that it should be broken up into smaller pieces!
def unpackJSON(myjson_fol):
    chem_df=googleio.ChemicalData()  #Grabs relevant chemical data frame from google sheets (only once no matter how many runs)
    concat_df_raw=pd.DataFrame() 
    print('Unpacking JSONs  ..', end='', flush=True)
    for file in sorted(os.listdir(myjson_fol)):
        if file.endswith(".json"):
            modlog.info('Unpacking %s' %file)
            concat_df=pd.DataFrame()  
            #appends each run to the original dataframe
            myjson=(os.path.join(myjson_fol, file))
            workflow1_json = json.load(open(myjson, 'r'))
            #gathers all information from raw data in the JSON file
            tray_df=parser.reagentparser(workflow1_json, myjson, chem_df) #generates the tray level dataframe for all wells including some calculated features
            concat_df=pd.concat([concat_df,tray_df], ignore_index=True, sort=True)
            #generates a well level unique ID and aligns
            runID_df=pd.DataFrame(data=[concat_df['_raw_jobserial'] + '_' + concat_df['_raw_vialsite']]).transpose()
            runID_df.columns=['RunID_vial']
            #Gets the mmol of each CHEMICAL and returns them summed and uniquely indexed
            mmol_df=calcmmol.mmol_breakoff(tray_df, runID_df)
            #combines all operations into a final dataframe for the entire tray level view with all information
            concat_df=pd.concat([mmol_df, concat_df, runID_df], sort=True, axis=1)
        #Combines the most recent dataframe with the final dataframe which is targeted for export
        concat_df_raw = pd.concat([concat_df_raw,concat_df], sort=True)
        print('.', end='',flush=True)
    print(' unpacking complete!')
    return(concat_df_raw) #this contains all of the raw values from the processed JSON files.  No additional data has been calculated

def augmentdataset(raw_df):
    ''' Processes full dataset through a series of operations to add molarity, features, calculated values, etc

    Takes the raw dataset compiled from the JSON files of each experiment and 
    performs rudimentary operations including: calculating concentrations and
    adding features.

    *This needs to be broken out into a separate module with each task allocated
    a single script which can be edited independently
    '''
    rawdataset_df_filled = raw_df.fillna(0)  #ensures that all values are filled (possibly problematic as 0 has a value)
    dataset_calcs_fill_df = augmolarity(rawdataset_df_filled) 
    dataset_calcs_desc_fill_df = augdescriptors(dataset_calcs_fill_df)
    return(dataset_calcs_desc_fill_df)

def augmolarity(concat_df_final):
    ''' Perform exp object molarity calculations (ideal concentrations), grab organic inchi
    '''
    concat_df_final.set_index('RunID_vial', inplace=True)
    #grabs all of the raw mmol data from the column header and creates a column which uniquely identifies which organic will be needed for the features in the next step
    inchi_df = concat_df_final.filter(like='_InChIKey')
    #takes all of the volume data from the robot run and reduces it into two total volumes, the total prior to FAH and the total after.  Returns a 3 column array "totalvol and finalvol in title"
    molarity_df=calcmolarity.molarity_calc(concat_df_final, finalvol_entries)
    #Sends off the final mmol list to specifically grab the organic inchi key and expose(current version)
    OrganicInchi_df=inchigen.GrabOrganicInchi(inchi_df, molarity_df)
    #Combines the new Organic inchi file and the sum volume with the main dataframe
    dataset_calcs_fill_df=pd.concat([OrganicInchi_df, concat_df_final, molarity_df], axis=1, join_axes=[concat_df_final.index])
    #Cleans the file in different ways for post-processing analysis
    return(dataset_calcs_fill_df)

def augdescriptors(dataset_calcs_fill_df):
    ''' bring in the inchi key based features for a left merge

    Temporary holder for processing the descriptors and adding them to the complete dataset.  
    If an amine is not present in the "perov_desc.csv1" file, the run will not be processed
    and will error out silently!  This is a feature not a bug (for now)  
    '''
    with open('data/perov_desc_edited.csv', 'r') as my_descriptors:
       descriptor_df=pd.read_csv(my_descriptors) 
    dirty_full_df=dataset_calcs_fill_df.merge(descriptor_df, left_on='_rxn_organic-inchikey', right_on='_raw_inchikey', how='inner')
    runID_df_big=pd.DataFrame(data=[dirty_full_df['_raw_jobserial'] + '_' + dirty_full_df['_raw_vialsite']]).transpose()
    runID_df_big.columns=['RunID_vial']
    dirty_full_df=pd.concat([runID_df_big, dirty_full_df], axis=1)
    dirty_full_df.set_index('RunID_vial', inplace=True)
    return(dirty_full_df)

def printfinal(myjsonfolder, debug,raw):
    modlog.info('%s loaded with JSONs for parsing, starting' %myjsonfolder)
    raw_df=unpackJSON(myjsonfolder)
    modlog.info('augmenting parsed JSONs with chemical calculations (concentrations)')
    augmented_raw_df = augmentdataset(raw_df)
    modlog.info('appending features and curating dataset')
    cleaned_augmented_raw_df= cleaner(augmented_raw_df, raw)
    finaloutcsv_name = myjsonfolder+'.csv'
    with open(finaloutcsv_name, 'w') as outfile:
        print('2d dataframe rendered successfully')
        cleaned_augmented_raw_df.to_csv(outfile)
    return(finaloutcsv_name)