import os
import re
import pandas as pd

from expworkup.devconfig import valid_input_files, workup_targets, lab_vars
from utils.globals import get_debug_header, get_debug_simple

def get_interface_filename(interface_type, working_directory, runID):
    """ Searches for filename match and returns instance

    Specified in devconfig['valid_input_files'] 
    new file names (suffixes) can be added in devconfig as needed

    Parameters
    ----------
    working_directory : (aka save_directory) where local files are
        report default = {target_directory}/gdrive_files
    
    runID :  name of gdrive folder containing the experiment
        aka. experiment_name,  e.g. 2019-09-18T20_27_33.741387+00_00_LBL

    Returns
    -------
    filename : identified filename for a particular type of file
        e.g. type = 'experiment_specification' could be 
                    'ExperimentSpecification.xls' or 'RobotInput.xls'
    """
    for suffix in valid_input_files[interface_type]:
        filename = os.path.join(working_directory, f'{runID}_{suffix}')
        if os.path.exists(filename):
            return filename

    raise FileNotFoundError(f'Could not find any of {valid_input_files[interface_type]} file for {runID}')

def get_experimental_run_lab(run_filename):
    """ parses experiment foldername and returns lab

    Parameters
    ----------
    run_filename: either the remote run directory name or the local json that is generated from it

    Returns
    -------
    labname
    """
    for lab in lab_vars.keys():
        lab_pat = re.compile(f'_({lab})($|.json$)')
        labname = lab_pat.search(run_filename.strip()) #returns if match
        if labname:
            return labname.group(1)

    raise RuntimeError(f'{run_filename} does not specify a supported lab')

def write_debug_file(df, filename, write_index=True):
    if os.path.isfile(filename):
        os.remove(filename)
    f = open(filename, 'a')
    if not get_debug_simple():
        f.write(get_debug_header())
        df.to_csv(f, index=write_index)
        f.write(get_debug_header())
    else:
        df.to_csv(f, index=write_index)
    f.close()


def get_command_dict(one_type, application):
    """Converts expworkup.type_command.csv to dict for chemdescriptor

    Parameters
    ----------
    one_type : defines which chemical type to target
        should match an entry in command_types_df 'types' column 
    
    application : defines the application being targeted by caller
        will only return rows where actor_systemtool_name matches 
        specified application

    Returns
    -------
    default_command_dict : structure shown below
        default_command_dict = {
        "descriptors": {
            "acceptorcount": {
                "command": [
                    "acceptorcount"
                ],
                "column_names": [
                    "_feat_acceptorcount"
                ]
            },...
        ""ph_descriptors": {
            "molsurfaceareaASAp": {
                "command": [
                    "molecularsurfacearea",
                    "-t",
                    "ASA+"
                ],
                "column_names": [
                    "_feat_molsurfaceareaASAp"
                ]
            },...

    Notes
    -----
     * https://github.com/darkreactions/chemdescriptor
     * 'descriptors' must be specified fully (including flags where needed)
     * 'ph_descriptors' are those which have -H option, can use to simplify return
    """
    command_type_df = pd.read_csv('./type_command.csv')
    if one_type == 'any':
        commands_df = command_type_df[(command_type_df['actor_systemtool_name'] == application)]
    else:
        commands_df = command_type_df[(command_type_df['input'] == one_type) &  \
                                      (command_type_df['actor_systemtool_name'] == application)]
    my_descriptor_dict = {}
    for command in commands_df.itertuples():
            column_name = f'_feat_{command.short_name}'
            my_descriptor_dict[command.short_name] = {}

            # 'space' (i.e, ' ') removal
            templist = command.calc_definition.split(' ')
            str_list = list(filter(None, templist))
            my_descriptor_dict[command.short_name]["command"] = str_list
            my_descriptor_dict[command.short_name]["column_names"] = [column_name]
            my_descriptor_dict[command.short_name]["alternative_input"] = command.alternative_input

    command_dict = {}
    command_dict['descriptors'] = my_descriptor_dict
    command_dict['ph_descriptors'] = {} # possibly useful, see chemdescriptor for more details
    if len(command_dict['descriptors'].keys()) == 0:
        return None
    else:
        return(command_dict)