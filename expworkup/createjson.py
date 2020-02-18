#Copyright (c) 2018 Ian Pendleton - MIT License
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
from validation import validation
from utils import globals
from utils.file_handling import get_interface_filename, get_experimental_run_lab

# todo put in config
## Set the workflow of the code used to generate the experimental data and to process the data
WorkupVersion = 1.1
modlog = logging.getLogger('report.CreateJSON')


def parse_preparation_interface(fname):
    """

    :param fname:
    :return:
    """
    with open(fname, "r") as f:
        exp_dict = json.load(f)
        exp_str = json.dumps(exp_dict, indent=4, sort_keys=True)
    f.close()
    exp_str = exp_str[:-8]  # remove the end of the json structure from the preparation interface dump, makes concatenation later easy
    return exp_str, exp_dict


def parse_exp_volumes(fname, experiment_lab):
    """

    :param fname:
    :return:
    """

    robot_dict = pd.read_excel(open(fname, 'rb'), header=[0], sheet_name=0)
    reagentlist = []
    for header in robot_dict.columns:
        if config.lab_vars[experiment_lab]['reagent_alias'] in header and "ul" in header:
            reagentlist.append(header)
    rnum = len(reagentlist)

    if experiment_lab == 'MIT_PVLab':
        rnum += 1

    pipette_volumes = pd.read_excel(fname, sheet_name=0,
                                    usecols=range(0, rnum+2))
    reaction_parameters = pd.read_excel(fname, sheet_name=0,
                                        usecols=[rnum+2, rnum+3]).dropna()
    reagent_info = pd.read_excel(fname, sheet_name=0,
                                 usecols=[rnum+4, rnum+5, rnum+6, rnum+7]).dropna()

    pipette_dump = json.dumps(pipette_volumes.values.tolist())
    reaction_dump = json.dumps(reaction_parameters.values.tolist())
    reagent_dump = json.dumps(reagent_info.values.tolist())

    return pipette_dump, reaction_dump, reagent_dump, pipette_volumes, reaction_parameters, reagent_info

def parse_observation_interface(fname):
    '''
    Gather the crystal datafile information and return JSON object

    :param fname: tabular file (.csv) used to contain experiment data
    :return:
    '''
    observation_df = pd.read_csv(fname)
    out_json = observation_df.to_json(orient='records')
    return out_json, observation_df

def parse_run_to_json(outfile, local_data_directory, run_name):
    """Parse data from one ESCALATE run into json and write to

    TODO: this is the T in ETL,
        * separate out the (Download, Read, (these are extract)), Transform, and Validate functionalty
            * It seems like we are going to need to Transform before we Validate
            * Question: are we reading from drive and validating, or reading from drive, saving to disk, and validating?
            * Right now we are running into datatype issues (e.g. int vs string) which are small trasnformations,
              Lets just (try to) transform everything into the expected form before validating

        * Document: how are we transforming it here?

    :param outfile:
    :param local_data_directory:
    :param run_name:
    :param crystal_data:
    :return:
    """

    local_data_directory = os.path.join('.', local_data_directory)
    run_lab = get_experimental_run_lab(run_name)

    exp_volume_spec_fname = get_interface_filename('experiment_specification', local_data_directory, run_name)
    prep_interface_fname = get_interface_filename('preparation_interface', local_data_directory, run_name)
    obs_interface_fname = get_interface_filename('observation_interface', local_data_directory, run_name)

    exp_str, exp_dict = parse_preparation_interface(prep_interface_fname)

    pipette_dump, reaction_dump, reagent_dump, \
    pipette_volumes, reaction_parameters, reagent_info = parse_exp_volumes(exp_volume_spec_fname, run_lab)

    crys_str, crys_df = parse_observation_interface(obs_interface_fname)

#    validation.validate_crystal_scoring(crys_df)
#    validation.validate_robot_input(pipette_volumes, reaction_parameters, reagent_info)
#    validation.validate_exp_data(exp_dict)

    print(exp_str, file=outfile)
    print('\t},', file=outfile)
    print('\t', '"well_volumes":', file=outfile)
    print('\t', pipette_dump, ',', file=outfile)
    print('\t', '"tray_environment":', file=outfile)
    print('\t', reaction_dump, ',', file=outfile)
    print('\t', '"robot_reagent_handling":', file=outfile)
    print('\t', reagent_dump, ',', file=outfile)
    print('\t', '"crys_file_data":', file=outfile)
    print('\t', crys_str, file=outfile)
    print('}', file=outfile)


def download_experiment_directories(target_directory, debug):
    """Gets all of the relevant folder titles from the experimental directory
    Cross references with the working directory of the final Json files send the list of jobs needing processing

    :param target_directory: The local directory in which the curated json files will be stored. From CLI.
    :param debug: 1 if debug mode, else 0. From CLI.
    :return:
    """
    save_directory = f'{target_directory}/gdrive_files'  # Local storage for gdrive files
    modlog.info('ensuring directories')
    if not os.path.exists(target_directory):
        os.mkdir(target_directory)
    if not os.path.exists(save_directory):
        os.mkdir(save_directory)
    
    #observation_UIDs, exp_volume_UIDs, prep_UIDs, 
    #googleio.gdrive_pipeline(config.lab_vars[globals.get_lab()]['remote_directory'])

    exp_dict = googleio.parse_gdrive_folder(config.lab_vars[globals.get_lab()]['remote_directory'], save_directory)

    modlog.info('Starting Download and Directory Parsing')
    print('Starting Download and Directory Parsing...')
    for exp_name, exp_files in tqdm(exp_dict.items()):
        run_json_filename = Path(target_directory + "/{}.json".format(exp_name))
        if os.path.isfile(run_json_filename):
            if os.stat(run_json_filename).st_size == 0:
                os.remove(run_json_filename)
                modlog.info('{} was empty and was removed'.format(json))
        while not os.path.isfile(run_json_filename):
            sleep_timer = 0
            try:
                googleio.gdrive_download(save_directory, exp_name, exp_files)
                outfile = open(run_json_filename, 'w')
                parse_run_to_json(outfile, save_directory, exp_name)
                outfile.close()
            except APIError as e:
                modlog.info(e.response)
                modlog.info(sys.exc_info())
                modlog.info('During download of {} sever request limit was met at {} seconds'.format(run_json_filename, sleep_timer))
                sleep_timer = 15.0

            modlog.info('New sleep timer {}'.format(sleep_timer))
            time.sleep(sleep_timer)
    return exp_dict


