import pandas as pd
import logging
from utils import globals

from expworkup.entity_tables.reagent_entity import ReagentObject

modlog = logging.getLogger('report.parser')

#Overview
#parases each index of the json file and returns a normalized data frame with each experiment (well) containing all relevant information
# TODO: report these from dictionary and generalize
def dict_listoflists(tray_environment_lists):
    """
    parses the tray ennvironment list (See example)

    Parameters
    -----------

    tray_environment_lists : list from the csv parser, describes the actions of a run
    [["Spincoating Temperature ( C )", 85.0], 
    ["Spincoating Speed (rpm):", 750.0], 
    ["Spincoating Duration (s)", 900.0], 
    ["Spincoating Duration 2 (s)", 1200.0], 
    ["Annealing Temperature ( C )", 105.0], 
    ["Annealing Duration (s)", 21600.0], 
    ["Test Action 1", 1000.0]]

    Returns
    --------
    tray_df : dataframe of the tray list
    """
    values = []
    for item in tray_environment_lists:
        value = item[1]
        values.append(value)
    tray_df = pd.DataFrame(values).transpose()

    # parses actions from robot files without having to specify things individually.  
    # Headers are named the same as the string of the action description (action name)
    tray_df.columns = [f"_rxn_{item[0].replace(' ','').replace('(','_').replace(')', '').replace(':','')}" for item in tray_environment_lists]
    # todo: handle custom parameters
    return(tray_df)

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

def flatten_json_reg(y):
    '''
    parases each index of the json file and returns a normalized data frame with each experiment (well) containing all relevant information

    :param y: input json from 'reagent' key of createjson.py 

    :return: dataframe of reagent data
    '''

    out = {}
    def flatten(x, flatdict, name=''):
        #Flattens the dictionary type to a single column and entry
        if type(x) is dict:
            for a in x:
                flatten(x[a], flatdict, name + a + '_')
        #Flattens list type to a single column and entry
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, flatdict, name + str(i) + '_')
                i += 1
        else:
            ## Parses the namespace if the units are contained with the value from the JSON
            if ':' in str(x):
                x1,x2=x.split(":")
                flatdict[str('_raw_reagent_' + name[:-1])] = x1 #Drops to the default namespace 
                flatdict[str('_raw_reagent_' + name[:-1] + '_units')] = str(x2) # adds a new entry for the column with the units which is adjacent to the measurement and otherwise has an identical name
            else:
                flatdict[str('_raw_reagent_' + name[:-1])] = str(x) #If the JSON value was a not a unit based value, the key value pair is returned as a column header and entry
    flatten(y, out) # Flattens the rest of the formating for pandas import
    namesdict = {}
    finaldict = {}
    # ensure reagents are correctly labeled 
    for k,v in out.items():
        if '_raw_reagent_' in k and '_id' in k:
            wrongkey = k.split('_')[:-1]
            wrongkey = ('_'.join(wrongkey))
            namesdict[wrongkey] = '_raw_reagent_%s'% (int(v)-1) #v is the new label once the reagents are updated
    for k,v in out.items():
#        print(k,v)
#        _raw_reagent_3_
        headerendlist = k.split('_')[4:]
        headerend = (('_'.join(headerendlist)))
        oldkeylist = (k.split('_')[:4])
        oldkey = (('_'.join(oldkeylist)))
        newkey = namesdict[oldkey] + '_' + headerend
        finaldict[newkey] = v
    return(pd.io.json.json_normalize(finaldict)) # normalizes the data and reads into a single row data frame

def reagentparser(firstlevel, myjson, chem_df):
    """

    """
    for reg_key,reg_value in firstlevel.items():
        modlog.info('Parsing %s to csv' %myjson)

        if reg_key == 'reagent':
            reagent_df = flatten_json_reg(reg_value)

        if reg_key == 'run':
            run_df=flatten_json(reg_value)

        if reg_key == 'tray_environment':
            tray_df=dict_listoflists(reg_value)
#        if reg_key == 'robot_reagent_handling':
#            robo_df=robo_handling(reg_value)

        if reg_key == 'well_volumes':
            reagenttotal=(len(reg_value[0])-2)
            listcount = 0
            columnnames = []
            columnnames.append('_raw_vialsite')
            while listcount < reagenttotal:
                columnnames.append('_raw_reagent_%s_volume'%listcount)
                listcount+=1
            columnnames.append('_raw_labwareID')
            well_volumes_df=pd.DataFrame(reg_value, columns=columnnames)

        if reg_key == 'crys_file_data':
            crys_file_data_df = pd.DataFrame(reg_value)
            if 'Concatenated Vial site' in crys_file_data_df.columns:
                crys_file_data_df = crys_file_data_df.rename(columns = {'Concatenated Vial site': '_raw_vialsite'})
                experiment_df=well_volumes_df.merge(crys_file_data_df)

            #The following code aligns and normalizes the data frames
            elif 'Experiment Number' in crys_file_data_df.columns:
                crys_file_data_df['_raw_vialsite'] = crys_file_data_df['Experiment Number'].astype(int)
                well_volumes_df.dropna(inplace=True)
                well_volumes_df['_raw_vialsite'] = well_volumes_df['_raw_vialsite'].astype(int) 
#                experiment_df=well_volumes_df.merge(crys_file_data_df, on='_raw_vialsite')
                experiment_df = pd.concat([well_volumes_df.set_index('_raw_vialsite'),crys_file_data_df.set_index('_raw_vialsite')], axis=1, join='inner').reset_index()
                experiment_df['_raw_vialsite'] = experiment_df['_raw_vialsite'].astype(str)

    #The following code aligns and normalizes the data frames
    wellcount=(len(experiment_df.index))-1
    fullrun_df=(run_df.append([run_df]*wellcount,ignore_index=True))
    fullreagent_df=(reagent_df.append([reagent_df]*wellcount,ignore_index=True))
    fulltray_df=(tray_df.append([tray_df]*wellcount, ignore_index=True))
    out_df=pd.concat([experiment_df, fullreagent_df, fullrun_df, fulltray_df], axis=1)
    return(out_df)
