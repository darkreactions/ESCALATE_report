#Copyright (c) 2018 Ian Pendleton - MIT License
import os
import sys
import time
import pandas as pd
import numpy as np
import logging
import json
from pathlib import Path
from gspread.exceptions import APIError

from tqdm import tqdm

import expworkup.devconfig as config
from expworkup import googleio
from validation import validation
from utils import globals
from utils.file_handling import get_interface_filename, get_experimental_run_lab

# todo put in config
## Set the workflow of the code used to generate the experimental data and to process the data
WorkupVersion = 1.0
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


def download_experiment_directories(local_directory, debug):
    """Gets all of the relevant folder titles from the experimental directory
    Cross references with the working directory of the final Json files send the list of jobs needing processing

    :param local_directory: The local directory to which to download data. From CLI.
    :param debug: 1 if debug mode, else 0. From CLI.
    :return:
    """

    modlog.info('starting directory parsing')

    observation_UIDs, exp_volume_UIDs, prep_UIDs, drive_run_dirnames = googleio.get_drive_UIDs(config.lab_vars[globals.get_lab()]['remote_directory'])

    print('Building folders ...')
    for drive_run_dirname in tqdm(drive_run_dirnames):
        sleep_timer = 1
        run_json_filename = Path(local_directory + "/{}.json".format(drive_run_dirname))

        if os.path.exists(run_json_filename):
            if os.stat(run_json_filename).st_size == 0:
                os.remove(run_json_filename)
                modlog.info('{} was empty and was removed'.format(run_json_filename))
            else:
                continue

        while not os.path.exists(run_json_filename):

            time.sleep(sleep_timer)
            outfile = open(run_json_filename, 'w')
            workdir = 'data/datafiles/'  # todo ian whats up with this?
            modlog.info('{} Created'.format(drive_run_dirname))
            # todo somehow I dont think having all of is info in separate dicts makes sense...
            # there should be a better way to pass all of this data around
            """
            Something like: 
            UIDs = {'run_name': {'crys': str, 'robo': str, 'exp': str}}
                """
            try:
                googleio.download_run_data(observation_UIDs[drive_run_dirname],
                                           exp_volume_UIDs[drive_run_dirname],
                                           prep_UIDs[drive_run_dirname],
                                           workdir,
                                           drive_run_dirname)
            except APIError as e:

                if not e.response.reason == 'Too Many Requests':
                    raise e

                modlog.info(sys.exc_info())
                modlog.info('During download of {} sever request limit was met at {} seconds'.format(run_json_filename, sleep_timer))
                sleep_timer = sleep_timer*2

                if sleep_timer > 60:
                    sleep_timer = 60
                    print("Something might be wrong.. if this message displays more than once kill job and try re-running")
                modlog.info('New sleep timer {}'.format(sleep_timer))

            else:
                parse_run_to_json(outfile, workdir, drive_run_dirname)
                outfile.close()
            finally:
                if os.path.exists(run_json_filename) and os.stat(run_json_filename).st_size == 0:
                    os.remove(run_json_filename)

    print('%s associated local files created' % globals.get_lab())
