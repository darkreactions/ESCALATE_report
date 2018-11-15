#Copyright (c) 2018 Ian Pendleton - MIT License
import json
import pandas as pd
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from operator import itemgetter
from expworkup.handlers import parser
from expworkup.handlers import calcmmol
from expworkup.handlers import calcmolarity
from expworkup.handlers import inchigen

debug = 0 #args.Debug
finalvol_entries=2 ## Hard coded number of formic acid entries at the end of the run (this needs fixing)

### General Setup Information ###
##GSpread Authorization information
scope= ['https://www.googleapis.com/auth/spreadsheets.readonly']
credentials = ServiceAccountCredentials.from_json_keyfile_name('expworkup/creds/creds.json', scope) 
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

#Will eventually create a dataframe from the robot handling information
def robo_handling():
    pass

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
def cleaner(dirty_df):
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

## Unpack logic
    #most granular data for each row of the final CSV is the well information.
    #Each well will need all associated information of chemicals, run, etc. 
    #Unpack those values first and then copy the generated array to each of the invidual wells
    ### developed enough now that it should be broken up into smaller pieces!
def unpackJSON(myjson_fol):
    chem_df=(ChemicalData())  #Grabs relevant chemical data frame from google sheets (only once no matter how many runs)
    concat_df_raw=pd.DataFrame()  
    for file in sorted(os.listdir(myjson_fol)):
        if file.endswith(".json"):
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
    return(concat_df_raw) #this contains all of the raw values from the processed JSON files.  No additional data has been calculated

#Processes full dataset through a series of operations to add molarity, features, calculated values, etc
def augmentdataset(raw_df):
    rawdataset_df_filled = raw_df.fillna(0)  #ensures that all values are filled (possibly problematic as 0 has a value)
    dataset_calcs_fill_df = augmolarity(rawdataset_df_filled) 
    dataset_calcs_desc_fill_df = augdescriptors(dataset_calcs_fill_df)
    return(dataset_calcs_desc_fill_df)

#Augment the dataset with molarity calculations
def augmolarity(concat_df_final):
    final_id=concat_df_final['RunID_vial']
    concat_df_final.set_index('RunID_vial', inplace=True)
    #grabs all of the raw mmol data from the column header and creates a column which uniquely identifies which organic will be needed for the features in the next step
    rxn_mmol_df=concat_df_final.filter(like='_raw_mmol_')
    #Sends off the final mmol list to generate inchi list and  for the chemical features calculations
    OrganicInchi_df=inchigen.GrabInchi(rxn_mmol_df, final_id)
    #takes all of the volume data from the robot run and reduces it into two total volumes, the total prior to FAH and the total after.  Returns a 3 column array "totalvol and finalvol in title"
    molarity_df=calcmolarity.molarity_calc(concat_df_final, finalvol_entries)
    #Combines the new Organic inchi file and the sum volume with the main dataframe
    dataset_calcs_fill_df=pd.concat([OrganicInchi_df, concat_df_final, molarity_df], axis=1, join_axes=[concat_df_final.index])
    #Cleans the file in different ways for post-processing analysis
    return(dataset_calcs_fill_df)

#Temporary holder for processing the descriptors and adding them to the complete dataset.  
#If an amine is not present in the "perov_desc.csv1" file, the run will not be processed
def augdescriptors(dataset_calcs_fill_df):
    #bring in the inchi key based features for a left merge
    with open('data/perov_desc.csv1', 'r') as my_descriptors:
       descriptor_df=pd.read_csv(my_descriptors) 
    dirty_full_df=dataset_calcs_fill_df.merge(descriptor_df, left_on='_rxn_organic-inchikey', right_on='_raw_inchikey', how='inner')
    runID_df_big=pd.DataFrame(data=[dirty_full_df['_raw_jobserial'] + '_' + dirty_full_df['_raw_vialsite']]).transpose()
    runID_df_big.columns=['RunID_vial']
    dirty_full_df=pd.concat([runID_df_big, dirty_full_df], axis=1)
    dirty_full_df.set_index('RunID_vial', inplace=True)
    return(dirty_full_df)

def printfinal(myjsonfolder):
    raw_df=unpackJSON(myjsonfolder)
    augmented_raw_df = augmentdataset(raw_df)
    cleaned_augmented_raw_df= cleaner(augmented_raw_df)
    with open('Final.csv', 'w') as outfile:
        print('Complete')
        cleaned_augmented_raw_df.to_csv(outfile)