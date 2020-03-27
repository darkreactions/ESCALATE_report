#Copyright (c) 2018 Ian Pendleton - MIT License
import json
import pandas as pd
import os
from operator import itemgetter
import logging

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from tqdm import tqdm

from tests import logger
from expworkup import googleio
from expworkup.handlers import parser
from expworkup.handlers import calcmmol
from expworkup.handlers import calcmolarity
from expworkup.handlers import inchigen
from utils.file_handling import get_experimental_run_lab
from utils import globals


debug = 0 #args.Debug
finalvol_entries=2 ## Hard coded number of formic acid entries at the end of the run (this needs fixing)

modlog = logging.getLogger('report.JSONtoCSV')


def robo_handling():
    '''
    TODO: Create a dataframe from the robot handling information 
    '''
    pass


def nameCleaner(sub_dirty_df, new_prefix):
    ''' The name cleaner is hard coded at the moment for the chemicals
    we are using at HC/ LBL
    TODO: Generalize name cleaner for groups or "m_types" based on inchikey
    or chemical abbreviation

    '''
    organic_df = pd.DataFrame()
    cleaned_M = pd.DataFrame()
    for header in sub_dirty_df.columns:
        # m_type = solvent (all solvent category data)
        if 'YEJRWHAVMIAJKC-UHFFFAOYSA-N' in header \
                or 'ZMXDDKWLCZADIW-UHFFFAOYSA-N' in header \
                or 'IAZDPXIOMUYVGZ-UHFFFAOYSA-N' in header \
                or 'YMWUJEATGCHHMB-UHFFFAOYSA-N' in header \
                or 'ZASWJUOMEGBQCQ-UHFFFAOYSA-L' in header \
                or 'UserDefinedSolvent' in header:  # This one is PbBr2 (just need to pass for now!)
            pass
        # m_type = acid
        elif "BDAGIHXWWSANSR-UHFFFAOYSA-N" in header:
            cleaned_M['%s_acid' % new_prefix] = sub_dirty_df[header]
        # m_type = inorganic (category of "inorgnic" used for HC/ LBL)
        elif 'RQQRAHKHDFPBMC-UHFFFAOYSA-L' in header:
            cleaned_M['%s_inorganic' % new_prefix] = sub_dirty_df[header]
        else:
            organic_df[header] = sub_dirty_df[header]
    cleaned_M['%s_organic' % new_prefix] = organic_df.sum(axis=1)
    return(cleaned_M)


def cleaner(dirty_df, raw, lab_list):
    ''' cleans up the name space and the csv output for distribution

    Cleans the file in different ways for post-processing analysis
    '''
    rxn_molarity_clean = nameCleaner(dirty_df.filter(like='_raw_M_'), '_raw_v0-M')
    rxn_v1molarity_clean = nameCleaner(dirty_df.filter(like='_raw_v1-M_'), '_rxn_M')
    print(lab_list)

    postprocess_1 = ['LBL', "HC", '4-Data-WF3_Iodide', '4-Data-Bromides', '4-Data-WF3_Alloying']
    if all(elem in postprocess_1 for elem in lab_list):
        # TODO: Clean up the headers for associated dataframes, move these functions to devconfig (onboarding a new lab)
        dirty_df.rename(columns={'Unnamed: 2': '_raw_placeholder'}, inplace=True)
        dirty_df.rename(columns={'Bulk Actual Temp (C)': '_rxn_temperatureC_actual_bulk'}, inplace=True)
        dirty_df.rename(columns={'Crystal Score': '_out_crystalscore'}, inplace=True)
        dirty_df.rename(columns={'_out_predicted': '_raw_model_predicted'}, inplace=True)
        dirty_df.rename(columns={'modelname': '_raw_modelname'}, inplace=True)
        dirty_df.rename(columns={'participantname': '_raw_participantname'}, inplace=True)
#        dirty_df.rename(columns={'notes': '_raw_notes'}, inplace=True)
        rxn_df = dirty_df.filter(like='_rxn_')
        rxn_df = dirty_df.filter(like='_rxn_')
        feat_df = dirty_df.filter(like='_feat_') 
        out_df = dirty_df.filter(like='_out_') 
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
    #TODO: default handling of specific labs
    elif all(elem in ['MIT_PVLab', 'dev'] for elem in lab_list):
        squeaky_clean_df = dirty_df
    #TODO: default handling of everything else
    else:
        squeaky_clean_df = dirty_df
    return(squeaky_clean_df)

def unpackJSON(myjson_fol, chemdf_dict):
    """
    most granular data for each row of the final CSV is the well information.
    Each well will need all associated information of chemicals, run, etc.
    Unpack those values first and then copy the generated array to each of the invidual wells
    developed enough now that it should be broken up into smaller pieces!

    #TODO: Break each of these off into tables for database
    :param myjson_fol:
    :return:
    """
    concat_df = pd.DataFrame()
    concat_df_raw = pd.DataFrame()

    json_list = []
    for my_exp_json in sorted(os.listdir(myjson_fol)):
        if my_exp_json.endswith(".json"):
            json_list.append(my_exp_json)
    print('Dataset download complete. Unpacking JSON Files...')
    for my_exp_json in tqdm(json_list):
        modlog.info('Unpacking %s' %my_exp_json)
        concat_df=pd.DataFrame()  
        #appends each run to the original dataframe
        myjson=(os.path.join(myjson_fol, my_exp_json))
        workflow1_json = json.load(open(myjson, 'r'))
        #gathers all information from raw data in the JSON file
        chem_df = chemdf_dict[get_experimental_run_lab(my_exp_json)]
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
    print('JSON to CSV conversion complete!')
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
    
    grabs all of the raw mmol data from the column header and creates a column which uniquely 
    identifies which organic will be needed for the features in the next step
    '''
    concat_df_final.set_index('RunID_vial', inplace=True)
    inchi_df = concat_df_final.filter(like='_InChIKey')

    #takes all of the volume data from the robot run and reduces it into two total volumes, the total prior to FAH and the total after.  Returns a 3 column array "totalvol and finalvol in title"
    molarity_df=calcmolarity.molarity_calc(concat_df_final, finalvol_entries)

    #Sends off the final mmol list to specifically grab the organic inchi key and expose(current version)
    OrganicInchi_df=inchigen.GrabOrganicInchi(inchi_df, molarity_df)
    
    #Combines the new Organic inchi file and the sum volume with the main dataframe
    dataset_calcs_fill_df=pd.concat([OrganicInchi_df, concat_df_final, molarity_df], axis=1, join_axes=[concat_df_final.index])
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
    my_descriptors.close()
    return(dirty_full_df)

def printfinal(myjsonfolder, raw_bool_cli, chemdf_dict, dataset_list):
    '''Top level json parser pipeline

    :param myjsonfolder: target folder for run and the generated data
    :param raw_bool_cli: list of cli arguments 

    Returns
    ---------
    cli_specified_name : final csv file name from dataset generation

    '''
    finaloutcsv_filename = myjsonfolder+'.csv'
    cli_specified_name = myjsonfolder

    modlog.info('%s loaded with JSONs for parsing, starting' %myjsonfolder)
    raw_df=unpackJSON(myjsonfolder, chemdf_dict)
    modlog.info('augmented dataframe with chemical calculations (concentrations)')

    augmented_raw_df = augmentdataset(raw_df)
    modlog.info(f'Appending physicochemical features to {finaloutcsv_filename} dataframe')
    print(f'Appending physicochemical features to {finaloutcsv_filename} dataframe')

    cleaned_augmented_raw_df= cleaner(augmented_raw_df, raw_bool_cli, dataset_list)
    modlog.info(f'Renaming dataframe headers')
    print('Renaming dataframe headers')

    with open(finaloutcsv_filename, 'w') as outfile:
        cleaned_augmented_raw_df.to_csv(outfile)
        print(f'{finaloutcsv_filename} rendered successfully')
        outfile.close()
    return(cli_specified_name)