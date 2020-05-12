#Copyright (c) 2020 Ian Pendleton - MIT License
import os
import sys
import time
import pandas as pd
import numpy as np
import logging
import json
from pathlib import Path

from tqdm import tqdm

import expworkup.devconfig as config
from gspread.exceptions import APIError
from expworkup import googleio
from utils.globals import lab_safeget, WARNCOUNT
from tests.validation.validation import validate_observation_interface, validate_experimental_volumes, validate_reaction_parameters
from tests.validation.validation import validate_reagent_info, validate_ingredient_data, validate_is_json
from utils.file_handling import get_interface_filename, get_experimental_run_lab

## Set the workflow of the code used to generate the experimental data and to process the data

modlog = logging.getLogger(f'mainlog.{__name__}')
warnlog = logging.getLogger(f'warning.{__name__}')

def parse_preparation_interface(prep_interface_fname):
    """ reads in json preparation interface, structure for appending

    Parameters
    ----------
    prep_interface_fname: filename of preparation inteface
        e.g., 2018-12-04T01_52_47.768958+00_00_LBL_ExpDataEntry.json

    Returns
    -------
    exp_str: json w/o final characters
        makes appending to other files easier

    exp_dict: complete json

    """
    with open(prep_interface_fname, "r") as f:
        exp_dict = json.load(f)
        exp_str = json.dumps(exp_dict, indent=4, sort_keys=True)
    f.close() 

    validate_ingredient_data(exp_dict)

    # Remove the end of the json structure from the preparation interface dump, 
    # This makes concatenation with other files easier
    exp_str = exp_str[:-8] 
    return exp_str 


def parse_exp_volumes(exp_volume_spec_fname, experiment_lab):
    """ Parses the experiment interface.  
        For example of the complete structure see:
        https://drive.google.com/open?id=1rNPfcOiseQSTTB8E7VsKp77h8hv8VSCj
        Link targets 4-DataDebug Robotinput example

    Parameters
    ----------
    exp_volume_spec_fname  : filename which contains volumes actions
        predifined structure is hardcoded based on a specific labs needs

    experiment_lab : name of lab
        all labs except MIT are handled the same, this is hard coded and brittle
    
    Returns
    -------
    lists and dataframes, complex return

    Notes:
    TODO: Generalize this function to any new lab
    TODO: clean up the return of this function
    If this function breaks pytest will catch the malfunction

    """

    robot_dict = pd.read_excel(open(exp_volume_spec_fname, 'rb'), header=[0], sheet_name=0)
    reagentlist = []
    for header in robot_dict.columns:
        reagent_alias_name = lab_safeget(config.lab_vars, experiment_lab, 'reagent_alias')
        if reagent_alias_name in header and "ul" in header:
            reagentlist.append(header)
    rnum = len(reagentlist)

    pipette_list = range(0,rnum+2)

    #MIT_PVLab has an additional column in the second row 'Experiment Name' in additiona
    # to the 'Experiment Index'.  The +1 accounts for that during parsing
    if experiment_lab == 'MIT_PVLab':
        rnum += 1
        pipette_list = [0]
        pipette_list.extend(range(2,rnum+2))

    pipette_volumes = pd.read_excel(exp_volume_spec_fname, sheet_name=0,
                                    usecols=pipette_list)
    pipette_volumes.dropna(how='all', inplace=True)

    reaction_parameters = pd.read_excel(exp_volume_spec_fname, sheet_name=0,
                                        usecols=[rnum+2, rnum+3]).dropna()
    reagent_info = pd.read_excel(exp_volume_spec_fname, sheet_name=0,
                                 usecols=[rnum+4, rnum+5, rnum+6, rnum+7]).dropna()

    validate_experimental_volumes(pipette_volumes)
    validate_reaction_parameters(reaction_parameters)
    validate_reagent_info(reagent_info)

    pipette_dump = json.dumps(pipette_volumes.values.tolist())
    reaction_dump = json.dumps(reaction_parameters.values.tolist())
    reagent_dump = json.dumps(reagent_info.values.tolist())
    return pipette_dump, reaction_dump, reagent_dump 

def parse_observation_interface(fname):
    '''
    Gather the crystal CSV information and return JSON object

    Cleans and validates on import

    Parameters
    ----------
    fname: target tabular file (.csv) used to contain experiment data

    Returns
    -------
    out_json : json of the tabular data

    observation_df : tabular data rendered to dataframe
    '''
    global WARNCOUNT
    observation_df = pd.read_csv(fname)
    observation_df_temp = observation_df.copy()
    # Normalized dataframe from user entry
    if 'modelname' in observation_df.columns and 'participantname' in observation_df.columns:
        observation_df.dropna(how='all', inplace=True, subset=['modelname', 'participantname'])
        # Some will just not have any entries, we don't want to nuke, but warn them that there is an issue
        if observation_df.shape[0] == 0:
            modlog.info(f"VALIDATION ERROR: {fname} does not have modelname or participantname information, please correct!")
            observation_df = observation_df_temp
            if WARNCOUNT == 0:
                warnlog.warn('Files failed to validate. Please search for "validation" log.')
                WARNCOUNT += 1
    if 'Crystal Score' in  observation_df.columns:
        observation_df = observation_df.astype({'Crystal Score': int})

    validate_observation_interface(observation_df)

    out_json = observation_df.to_json(orient='records')
    return out_json 

def parse_run_to_json(outfile, working_directory, experiment_name):
    """Parse downloaded files from one experiment into a summary json file

    Parameters
    ----------
    outfile : target json file to write parsed experimental data

    working_directory : (aka save_directory) where local files are
        report default = {target_directory}/gdrive_files
    
    experiment_name :  name of gdrive folder containing the experiment
        aka. runUID,  e.g. 2019-09-18T20_27_33.741387+00_00_LBL

    Return
    ------
    None

    """
    working_directory = os.path.join('.', working_directory)
    run_lab = get_experimental_run_lab(experiment_name)

    exp_volume_spec_fname = get_interface_filename('experiment_specification',
                                                   working_directory,
                                                   experiment_name)
    prep_interface_fname = get_interface_filename('preparation_interface',
                                                  working_directory,
                                                  experiment_name)
    obs_interface_fname = get_interface_filename('observation_interface',
                                                 working_directory,
                                                 experiment_name)

    # All functions return JSON structure
    ingredient_prep_str  = parse_preparation_interface(prep_interface_fname)
    pipette_dump, reaction_dump, reagent_dump = parse_exp_volumes(exp_volume_spec_fname, run_lab)
    observation_json = parse_observation_interface(obs_interface_fname)

    print(ingredient_prep_str, file=outfile)
    print('\t},', file=outfile)
    print('\t', '"well_volumes":', file=outfile)
    print('\t', pipette_dump, ',', file=outfile)
    print('\t', '"tray_environment":', file=outfile)
    print('\t', reaction_dump, ',', file=outfile)
    print('\t', '"robot_reagent_handling":', file=outfile)
    print('\t', reagent_dump, ',', file=outfile)
    print('\t', '"crys_file_data":', file=outfile)
    print('\t', observation_json, file=outfile)
    print('}', file=outfile)

def download_experiment_directories(target_directory, dataset):
    """Gets all of the relevant folder titles from the experimental directory
    
    Cross references with the working directory of the final Json files send
    the list of jobs needing processing

    Parameters
    ----------
    target_directory: target folder for storing the run and associated data
        The local directory in which the curated json files will be stored. From CLI.

    exp_dict : dict {<folder_name> : folder_children uids}
        dict keyed on the child foldernames with values being a list of the 
        items in the expeirment (child) gdrive folders 
        (list items objects are dictionaries with defined structure)
    """
    save_directory = f'{target_directory}/gdrive_files'  # Local storage for gdrive files
    modlog.info('ensuring directories')
    if not os.path.exists(save_directory):
        os.mkdir(save_directory)
    
    target_data_folder = config.workup_targets[dataset]['target_data_folder']
    exp_dict  = googleio.parse_gdrive_folder(target_data_folder, save_directory)

    modlog.info('Starting Download and Directory Parsing')
    print('(2/6) Starting Download and Directory Parsing...')
    for exp_name, exp_files in tqdm(exp_dict.items()):
        run_json_filename = Path(target_directory + "/{}.json".format(exp_name))
        if os.path.isfile(run_json_filename):
            if os.stat(run_json_filename).st_size == 0:
                os.remove(run_json_filename)
                modlog.info('{} was empty and was removed'.format(json))
        while not os.path.isfile(run_json_filename):
            sleep_timer = 0
            try:
                # Download files locally
                googleio.gdrive_download(save_directory, exp_name, exp_files)
                outfile = open(run_json_filename, 'w')
                # Parse them to JSON
                parse_run_to_json(outfile, save_directory, exp_name)
                outfile.close()
            except APIError as e:
                modlog.info(e.response)
                modlog.info(sys.exc_info())
                modlog.info('During download of {} sever request limit was met at {} seconds'.format(run_json_filename, sleep_timer))
                sleep_timer = 15.0

            is_valid_json = validate_is_json(run_json_filename)
            if not is_valid_json:
               os.remove(run_json_filename) 
               warnlog.warn(f'{run_json_filename} could not be properly constructed. Omitting from dataset. Please inspect run!')
               modlog.warn(f'{run_json_filename} could not be properly constructed. Omitting from dataset. Please inspect run!')

            modlog.info('New sleep timer {}'.format(sleep_timer))
            time.sleep(sleep_timer)
    return exp_dict

def inventory_assembly(exp_dict):
    """ 
    Gather chemical inventories used to generate runs for labs in exp_dict

    Parameters
    ----------
    exp_dict : dict {<folder_name> : folder_children uids}
        dict keyed on the child foldernames with values being all of the 
        grandchildren gdrive objects (these objects are dictionaries as 
        well with defined structure)

    Returns
    ----------
    chemdf_dict : dict {<lab name>: chemdf}
        labs specified in 'lab name' (keys) must be included in the 
        devconfig lab_vars dictionary the chemdf is targeted based on 
        the specifed google uid of the 'chemsheetid' key in the 
        lab_vars dictionary subsequent code will be automated to target
        the default if no lab specific chemdf uid is provided.
    """
    chemdf_dict = {}
    lablist = [] 
    for title in exp_dict.keys():
        run_lab = get_experimental_run_lab(title)
        if run_lab not in lablist:
            lablist.append(run_lab)
    
    for lab in lablist:
        chem_df = googleio.ChemicalData(lab)                    
        chemdf_dict[lab] = chem_df

    return chemdf_dict 