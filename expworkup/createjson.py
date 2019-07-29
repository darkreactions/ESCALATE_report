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

from expworkup import googleio
from validation import validation
from utils import globals

# todo put in config
## Set the workflow of the code used to generate the experimental data and to process the data
WorkupVersion = 1.0
modlog = logging.getLogger('report.CreateJSON')


def Expdata(local_ExpDataEntry_json):
    """

    :param local_ExpDataEntry_json:
    :return:
    """
    with open(local_ExpDataEntry_json, "r") as f:
        exp_dict = json.load(f)
        exp_str = json.dumps(exp_dict, indent=4, sort_keys=True)
    exp_str = exp_str[:-8]  # todo Ian: why? this needs to be documented
    return exp_str, exp_dict


def Robo(robotfile, robotfile1):
    """

    :param robotfile:
    :return:
    """
    # ooooh weeee that is some nasty code below, just looking for the right name
    try:
        robot_dict = pd.read_excel(open(robotfile, 'rb'), header=[0], sheet_name=0)
        reagentlist = []
        for header in robot_dict.columns:
            if 'Reagent' in header and "ul" in header:
                reagentlist.append(header)
        rnum = len(reagentlist)

        # todo: read once, then slice
        pipette_volumes = pd.read_excel(robotfile, sheet_name=0,
                                        usecols=range(0, rnum+2))
        reaction_parameters = pd.read_excel(robotfile, sheet_name=0,
                                            usecols=[rnum+2, rnum+3]).dropna()
        reagent_info = pd.read_excel(robotfile, sheet_name=0,
                                     usecols=[rnum+4, rnum+5, rnum+6, rnum+7]).dropna()

        pipette_dump = json.dumps(pipette_volumes.values.tolist())
        reaction_dump = json.dumps(reaction_parameters.values.tolist())
        reagent_dump = json.dumps(reagent_info.values.tolist())
    except OSError:
        try:
            robot_dict = pd.read_excel(open(robotfile1, 'rb'), header=[0], sheet_name=0)
            robotfile = robotfile1
            reagentlist = []
            for header in robot_dict.columns:
                if 'Precursor' in header and "ul" in header:
                    reagentlist.append(header)
            rnum = len(reagentlist)
            pipette_volumes = pd.read_excel(robotfile, sheet_name=0,
                                            usecols=range(0, rnum+3))
            reaction_parameters = pd.read_excel(robotfile, sheet_name=0,
                                                usecols=[rnum+3, rnum+4]).dropna()
            reagent_info = pd.read_excel(robotfile, sheet_name=0,
                                         usecols=[rnum+5, rnum+6, rnum+7, rnum+8]).dropna()
            pipette_dump = json.dumps(pipette_volumes.values.tolist())
            reaction_dump = json.dumps(reaction_parameters.values.tolist())
            reagent_dump = json.dumps(reagent_info.values.tolist())
        except OSError:
            modlog.error("Failed to find correct experiment specification file, severe exit error")
            sys.exit()
    return pipette_dump, reaction_dump, reagent_dump, pipette_volumes, reaction_parameters, reagent_info

def Crys(crysfile, crysfile1):
    '''
    Gather the crystal datafile information and return JSON object

    :param crysfile: tabular file (.csv) used to contain experiment data
    :return:
    '''
    #headers = crysfile.pop(0)
    try:
        crys_df_curated = pd.read_csv(crysfile)#, columns=headers)
    except Exception:
        try:
            crys_df_curated = pd.read_csv(crysfile1)#, columns=headers)
        except Exception:
            modlog.error("Failed to find correct experiment observation file. \
                         Neither %s or %s exist!" % (crysfile, crysfile1))
            sys.exit()
    out_json = crys_df_curated.to_json(orient='records')
    return(out_json, crys_df_curated)

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
    exp_filename = local_data_directory + run_name + '_ExpDataEntry.json'

    robo_filename = './' + local_data_directory + run_name + '_RobotInput.xls'
    robo_filename1 = './' + local_data_directory + run_name + '_ExperimentSpecification.xls'

    crys_filename = local_data_directory + run_name + '_CrystalScoring.csv'
    crys_filename1 = local_data_directory + run_name + '_observation_interface.csv'

    exp_str, exp_dict = Expdata(exp_filename)

    pipette_dump, reaction_dump, reagent_dump, \
    pipette_volumes, reaction_parameters, reagent_info = Robo(robo_filename, robo_filename1)

    crys_str, crys_df = Crys(crys_filename, crys_filename1)

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


def ExpDirOps(local_directory, debug):
    """Gets all of the relevant folder titles from the experimental directory
    Cross references with the working directory of the final Json files send the list of jobs needing processing

    :param local_directory: The local directory to which to download data. From CLI.
    :param debug: 1 if debug mode, else 0. From CLI.
    :return:
    """

    modlog.info('starting directory parsing')
    if globals.get_lab() in ['LBL', 'HC']:
        # todo this should not be hard coded
        modlog.info('debugging disabled, running on main data directory')
        remote_directory = '13xmOpwh-uCiSeJn8pSktzMlr7BaPDo7B'
    elif globals.get_lab() in ['dev']:
        # todo this also shouldnt be hard coded: put both in a config file
        modlog.warn('debugging enabled! targeting dev folder')
        remote_directory = '1rPNGq69KR7_8Zhr4aPEV6yLtB6V4vx7k'
    elif globals.get_lab() in ['MIT_PVLab']:
        modlog.info('Pulling from MIT datafolder')
        remote_directory = '1VNsWClt-ppg8ojUztDYssnSgfoe9XRhi'

    observation_UIDs, exp_volume_UIDs, prep_UIDs, drive_run_dirnames = googleio.get_drive_UIDs(remote_directory)

    # todo: what to do with these log statements? Do we drop this vocabulary
    # modlog.info('parsing EXPERIMENTAL_OBJECT')
    # modlog.info('parsing EXPERIMENTAL_MODEL')
    # modlog.info('parsing REAGENT_MODEL_OBJECT')
    # modlog.info('building runs in local directory')

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
            try:
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
                googleio.download_run_data(observation_UIDs[drive_run_dirname],
                                           exp_volume_UIDs[drive_run_dirname],
                                           prep_UIDs[drive_run_dirname],
                                           workdir,
                                           drive_run_dirname)

                parse_run_to_json(outfile, workdir, drive_run_dirname)
                outfile.close()
                '''
                due to the limitations of the haverford googleapi 
                we have to throttle the connection a bit to limit the 
                number of api requests anything lower than 2 bugs it out

                This will need to be re-enabled once we open the software beyond
                haverford college until we improve the scope of the googleio api
                '''
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

                if os.path.exists(run_json_filename) and os.stat(run_json_filename).st_size == 0:
                    os.remove(run_json_filename)

    print('%s associated local files created' % globals.get_lab())
