#Copyright (c) 2018 Ian Pendleton - MIT License
import os
import time
import pandas as pd
import numpy as np
import logging
import json
from pathlib import Path

from tqdm import tqdm

from expworkup import googleio

## Set the workflow of the code used to generate the experimental data and to process the data
WorkupVersion=1.0

modlog = logging.getLogger('report.CreateJSON')

def Expdata(DatFile):
    ExpEntry=DatFile
    with open(ExpEntry, "r") as file1:
        file1out=json.load(file1)
        lines=(json.dumps(file1out, indent=4, sort_keys=True))
    lines=lines[:-8]
    return(lines)
    ## File processing for the experimental JSON to convert to the final form (header of the script)

def Robo(robotfile):
    #o the file handling for the robot.xls file and return a JSON object
    robot_dict = pd.read_excel(open(robotfile, 'rb'), header=[0], sheet_name=0)
    reagentlist = []
    for header in robot_dict.columns:
        if 'Reagent' in header and "ul" in header:
            reagentlist.append(header)
    rnum = len(reagentlist)
    robo_df = pd.read_excel(open(robotfile,'rb'), sheet_name=0,usecols=range(0,rnum+2))
    robo_df_2 = pd.read_excel(open(robotfile,'rb'), sheet_name=0,usecols=[rnum+2,rnum+3]).dropna()
    robo_df_3 = pd.read_excel(open(robotfile,'rb'), sheet_name=0,usecols=[rnum+4,rnum+5,rnum+6,rnum+7]).dropna()
    robo_dump=json.dumps(robo_df.values.tolist())
    robo_dump2=json.dumps(robo_df_2.values.tolist())
    robo_dump3=json.dumps(robo_df_3.values.tolist())
    return(robo_dump, robo_dump2, robo_dump3)

def Crys(crysfile):
    ##Gather the crystal datafile information and return JSON object
    headers=crysfile.pop(0)
    crys_df=pd.DataFrame(crysfile, columns=headers)
    crys_df_curated=crys_df[['Concatenated Vial site', 'Crystal Score', 'Bulk Actual Temp (C)', 'modelname']]
    crys_list=crys_df_curated.values.tolist()
    crys_dump=json.dumps(crys_list)
    return(crys_dump)


def parse_run_to_json(outfile, local_data_directory, remote_run_directory, crystal_data):
    """Parse data from one ESCALATE run into json and write to

    :param outfile:
    :param local_data_directory:
    :param remote_run_directory:
    :param crystal_data:
    :return:
    """
    experimental_data_entry_json_filename = local_data_directory + remote_run_directory + '_ExpDataEntry.json'
    robot_input_excel_filename = local_data_directory + remote_run_directory + '_RobotInput.xls'

    # todo this right here bois is where the data needs validatin'
    exp_return = Expdata(experimental_data_entry_json_filename)
    robo_return = Robo(robot_input_excel_filename)
    crys_return = Crys(crystal_data)
    print(exp_return, file=outfile)
    print('\t},', file=outfile)
    print('\t', '"well_volumes":', file=outfile)
    print('\t', robo_return[0], ',', file=outfile)
    print('\t', '"tray_environment":', file=outfile)
    print('\t', robo_return[1], ',', file=outfile)
    print('\t', '"robot_reagent_handling":', file=outfile)
    print('\t', robo_return[2], ',', file=outfile)
    print('\t', '"crys_file_data":', file=outfile)
    print('\t', crys_return, file=outfile)
    print('}', file=outfile)


def ExpDirOps(local_directory, debug):
    """Gets all of the relevant folder titles from the experimental directory
    Cross references with the working directory of the final Json files send the list of jobs needing processing

    :param local_directory: The local directory to which to download data. From CLI.
    :param debug: 1 if debug mode, else 0. From CLI.
    :return:
    """
    modlog.info('starting directory parsing')
    if debug == 0:
        # todo this should not be hard coded
        modlog.info('debugging disabled, running on main data directory')
        remote_directory = '13xmOpwh-uCiSeJn8pSktzMlr7BaPDo7B'
    elif debug == 1:
        # todo this also shouldnt be hard coded: put both in a config file
        modlog.warn('debugging enabled! targeting dev folder')
        remote_directory = '1rPNGq69KR7_8Zhr4aPEV6yLtB6V4vx7k'

    crys_dict, robo_dict, Expdata, remote_run_directories = googleio.drivedatfold(remote_directory)

    # todo: what to do with these log statements? Do we drop this vocabulary
    # modlog.info('parsing EXPERIMENTAL_OBJECT')
    # modlog.info('parsing EXPERIMENTAL_MODEL')
    # modlog.info('parsing REAGENT_MODEL_OBJECT')
    # modlog.info('building runs in local directory')

    print('Building folders ..', end='', flush=True)
    for remote_run_directory in tqdm(remote_run_directories):
        run_json_filename = Path(local_directory + "/{}.json".format(remote_run_directory))
        if run_json_filename.is_file():
            modlog.info('{} exists'.format(remote_run_directory))
        else:
            outfile = open(run_json_filename, 'w')
            workdir = 'data/datafiles/'  # todo ian whats up with this?
            modlog.info('{} Created'.format(remote_run_directory))

            # todo somehow I dont think having all of is info in separate dicts makes sense...
            # there should be a better way to pass all of this data around

            data_from_drive = googleio.getalldata(crys_dict[remote_run_directory],
                                                  robo_dict[remote_run_directory],
                                                  Expdata[remote_run_directory],
                                                  workdir,
                                                  remote_run_directory)

            parse_run_to_json(outfile, workdir, remote_run_directory, data_from_drive)
            outfile.close()
            time.sleep(4)  #see note below
            '''
            due to the limitations of the haverford googleapi 
            we have to throttle the connection a bit to limit the 
            number of api requests anything lower than 2 bugs it out

            This will need to be re-enabled once we open the software beyond
            haverford college until we improve the scope of the googleio api
            '''
    print(' local directories created')
