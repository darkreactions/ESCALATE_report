import pandas as pd
import logging
from utils import globals

from expworkup.entity_tables.reagent_entity import ReagentObject
from utils.file_handling import get_experimental_run_lab

modlog = logging.getLogger('report.parser')

#Overview
#parases each index of the json file and returns a normalized data frame with each experiment (well) containing all relevant information
# TODO: report these from dictionary and generalize

def dict_listoflists(list_lists, run_lab):
    values = []
    for item in list_lists:
        value = item[1]
        values.append(value)
    tray_df = pd.DataFrame(values).transpose()

    # parses actions from robot files without having to specify things individually.  
    # Headers are named the same as the string of the action description (action name)
    tray_df.columns = [f"_rxn_{item[0].replace(' ','').replace('(','_').replace(')', '').replace(':','')}" for item in list_lists]

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

#Organizes all of the gathered reagent data by reagent and proceeds to send off for calculating molality                        
def ReagentConc(one_reagent_df, reagent):
    df=one_reagent_df
    index=0
    conc={}
    reagent=reagent[5:]
    #Calculateds the concentration of the REAGENTS being used in each experiment (signified largely by a single runID_vial combination)
    for m_type in df['m_type']:
        if m_type == 'null':
            pass
        if m_type == 'solvent':
            try:
                solv_mm=float(df.loc[(index,'molecularmass')])
                solv_d=float(df.loc[(index,'density')])
            except Exception:
                solv_mm=(df.loc[(index,'molecularmass')])
                solv_d=(df.loc[(index,'density')])
            solv_A=float(df.loc[(index,'amount')])
        index+=1
    index=0
    #calculates the values for the reagent concentrations in terms of the total solvent used in the preparation (for inorganic and organic components)
    for m_type in df['m_type']:
        if (m_type == 'inorg') or (m_type == 'org'):
            mm=float(df.loc[(index,'molecularmass')])
            A=float(df.loc[(index,'amount')])
            calculated_concentration=(A / mm ) / ( solv_A / 1000) ## Returns the value in mmolarity
#            name_space=('_rxn_' + reagent + "_chemical_" + str(index) + "_conc_" + (df.loc[(index,'InChiKey')]))
            name_space=('_raw_' + reagent + "_conc_" + (df.loc[(index,'InChiKey')]))
            conc[name_space]=calculated_concentration
        #Calculates the pure "density" of the compound and uses that in place of the solvated concentration using the amounts from the experiment
        if (m_type == 'pureliquid'):
            mm=float(df.loc[(index,'molecularmass')])
            A=float(df.loc[(index,'amount')])
            d=float(df.loc[(index,'density')])
            calculated_concentration=(d/mm*1000)
            name_space=('_raw_' + reagent + "_conc_" + (df.loc[(index,'InChiKey')]))
            conc[name_space]=calculated_concentration
        index+=1
    if conc == {}:
        return('null')
    else:
        return([conc])


def calcConc(reagentdf, reagent_spc):
    """ calculates concentration of reagents

    TODO: Move conc_cells calculation into ReagentObject Class
    TODO: move organization of reagent specification sheet from chemical dataframe into separate function
    TODO: single step organization of dataframes for concentration calculation
    TODO: LONG TERM -- reagent class constructed at initial JSON parsing

    :param reagentdf: dataframe of reagent information for a grouping of experiments
    :param reagent_spc: dataframe generated from chemical inventory indexed by reagent number for a group of experiments
    :return: dataframe of concentrations with columns sorted by inchikey and reagent
                (ex. '_raw_reagent_1_conc_UPHCENSIMPJEIS-UHFFFAOYSA-N')
    """
    reagent_spc.set_index(['parsed name'], inplace=True)
    reagent_list=[]
    conc_df=pd.DataFrame()
    for reag_chem, row in reagent_spc.iterrows():
        if reag_chem not in reagent_list:
            reagent_list.append(reag_chem)
        else:
            pass
    count = 0
    for reagent in reagent_list:
        count += 1
        for reag_chem, row in reagent_spc.iterrows():
            if reagent in reag_chem:
                chemical_list = []
                chemical_list.append(reagent_spc.at[reag_chem, 'name'])
                chemical_list.append(reagent_spc.at[reag_chem, 'InChIKey'])
                chemical_list.append(reagent_spc.at[reag_chem, 'density'])
                chemical_list.append(reagent_spc.at[reag_chem, 'm_type'])
                chemical_list.append(reagent_spc.at[reag_chem, 'molecular mass'])
                chemical_list.append(reagent_spc.at[reag_chem, 'amount'])
                chemical_list.append(reagent_spc.at[reag_chem, 'units'])
        onereagent_df = (pd.DataFrame(chemical_list).transpose())
        onereagent_df.columns = ['name','InChiKey', 'density', 'm_type',
                                    'molecularmass','amount','unit']
        conc_cells = (ReagentConc(onereagent_df, reagent))
        current_reagent = ReagentObject(onereagent_df, reagent)
        v1conc_cells = current_reagent.conc_v1
        if conc_cells == 'null':
            pass
        else:
            conc_cells_df = pd.DataFrame(conc_cells)
            v1conc_cells_df = pd.DataFrame(v1conc_cells)
            conc_df = pd.concat([conc_df, conc_cells_df, v1conc_cells_df], axis=1)
    return conc_df

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

def reag_info(reagentdf,chemdf):
    '''
        ### Eventually this section of code can be replaced by class objects which describe the reagent for each experiment in line
        ### Currenlty this code is assembling a dataframe which describes the relatinship between various reagents in order
        ### to perform subsequent calcualtions.  The inchi keys for example are hard coded and should be variable
        #TODO use types from teh chemdf to handle each of these sections with exceptions for parsing
    '''
    reagentlist=[]
    for item in list(reagentdf):
        name='null'
        mm='null'
        density='null'
        density_new='null'
        units='null'
        if ('_raw_reagent_' in item) and ('InChIKey' in item):
            for InChIKey in reagentdf[item]:
                m_type='null'
                parse_name=item[:-21]
                parse_chemical_name=item[:-9]
                # list of acids
                if InChIKey == "BDAGIHXWWSANSR-UHFFFAOYSA-N":
                    mm=(float(chemdf.loc[InChIKey,"Molecular Weight (g/mol)"]))# / float(chemdf.loc["FAH","Density            (g/mL)"])
                    name=((chemdf.loc[InChIKey,"Chemical Name"]))# / float(chemdf.loc["FAH","Density            (g/mL)"])
                    m_type='pureliquid' #Assigns an object type for later analysis and calculation of concentrations
                    try:
                        density=(float(chemdf.loc[InChIKey,"Density            (g/mL)"]))
                    except:
                        modlog.error("Abort run and check %s density details in google sheets" %name)
                        break
                # list of all solvents
                elif InChIKey == 'YEJRWHAVMIAJKC-UHFFFAOYSA-N' \
                        or InChIKey == 'ZMXDDKWLCZADIW-UHFFFAOYSA-N' \
                        or InChIKey == 'IAZDPXIOMUYVGZ-UHFFFAOYSA-N' \
                        or InChIKey == 'YMWUJEATGCHHMB-UHFFFAOYSA-N' \
                        or InChIKey == 'MVPPADPHJFYWMZ-UHFFFAOYSA-N' \
                        or InChIKey == 'UserDefinedSolvent':
                    try: 
                        mm=(float(chemdf.loc[InChIKey,"Molecular Weight (g/mol)"]))
                    except Exception:
                        mm=chemdf.loc[InChIKey,"Molecular Weight (g/mol)"]
                    name=((chemdf.loc[InChIKey,"Chemical Name"]))# / float(chemdf.loc["FAH","Density            (g/mL)"])
                    m_type='solvent'
                    try:
                        density=(float(chemdf.loc[InChIKey,"Density            (g/mL)"]))
                    except:
                        modlog.error("Abort run and check %s density details in google sheets" %name)
                        break
                # list of inorganics
                elif InChIKey == 'RQQRAHKHDFPBMC-UHFFFAOYSA-L' \
                        or InChIKey == 'ZASWJUOMEGBQCQ-UHFFFAOYSA-L':
                    mm=(float(chemdf.loc[InChIKey,"Molecular Weight (g/mol)"]))
                    name=((chemdf.loc[InChIKey,"Chemical Name"]))# / float(chemdf.loc["FAH","Density            (g/mL)"])
                    m_type='inorg'
                    try:
                        density=(float(chemdf.loc[InChIKey,"Density            (g/mL)"]))
                    except:
                        modlog.error("Abort run and check %s density details in google sheets" %name)
                        break
                #Null
                elif InChIKey == 'null':
                    pass
                #Organic
                else:
                    mm=(float(chemdf.loc[InChIKey,"Molecular Weight (g/mol)"]))
                    name=((chemdf.loc[InChIKey,"Chemical Name"]))
                    m_type='org'
                    try:
                        density = (float(chemdf.loc[InChIKey,"Density            (g/mL)"]))
                    except:
                        modlog.error("Abort run and check %s density details in google sheets" %name)
                        break
                for (item) in list(reagentdf):
                    if (parse_chemical_name in item) and ("actual_amount" in item) and not ("units" in item):
                        amount=(reagentdf.loc[0,item])
                    if (parse_chemical_name in item) and ("actual_amount" in item) and ("units" in item):
                        units=(reagentdf.loc[0,item])
            reagentlist.append((name, mm, density, parse_name, InChIKey, m_type, amount, units))
    reagentlist_df=(pd.DataFrame(reagentlist, columns=['name', 'molecular mass', 'density', 'parsed name', "InChIKey", "m_type", "amount", "units"]))
    return(reagentlist_df)

def reagentparser(firstlevel, myjson, chem_df):

    run_lab = get_experimental_run_lab(myjson)
    for reg_key,reg_value in firstlevel.items():
        modlog.info('Parsing %s to csv' %myjson)

        if reg_key == 'reagent':
            reagent_df = flatten_json_reg(reg_value)
            reagent_spec = reag_info(reagent_df,chem_df) #takes all of the infomration from the chem_df (online web information here) and puts it together with the the reagent information
            concentration_df = calcConc(reagent_df, reagent_spec) #takes the information abot the reagents and generates ... 

        if reg_key == 'run':
            run_df=flatten_json(reg_value)

        if reg_key == 'tray_environment':
            tray_df=dict_listoflists(reg_value, run_lab)
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
    fullConc_df=(concentration_df.append([concentration_df]*wellcount,ignore_index=True))
    fullreagent_df=(reagent_df.append([reagent_df]*wellcount,ignore_index=True))
    fulltray_df=(tray_df.append([tray_df]*wellcount, ignore_index=True))
    out_df=pd.concat([fullConc_df, experiment_df, fullreagent_df, fullrun_df, fulltray_df], axis=1)
    return(out_df)
