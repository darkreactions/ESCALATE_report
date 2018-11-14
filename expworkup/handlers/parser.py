import pandas as pd

#Overview
#parases each index of the json file and returns a normalized data frame with each experiment (well) containing all relevant information

def dict_listoflists(list_lists):
    values=[]
    for item in list_lists:
#        key=str(item[0])
#        key=str('_rxn_'+key[:-1])
#        keys.append(key)
        value=item[1]
        values.append(value)
    tray=pd.DataFrame(values)#, columns=['_rxn_temperatureC', '_rxn_stirrateRPM','_rxn_mixingtime1S','_rxn_mixingtime2S','_rxn_reactiontimeS'])
    tray_df=tray.transpose()
    tray_df.columns =['_rxn_temperatureC', '_rxn_stirrateRPM','_rxn_mixingtime1S','_rxn_mixingtime2S','_rxn_reactiontimeS']
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
            solv_mm=float(df.loc[(index,'molecularmass')])
            solv_d=float(df.loc[(index,'density')])
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
    reagent_spc.set_index(['parsed name'], inplace=True)
    reagent_list=[]
    conc_df=pd.DataFrame()
    for reag_chem, row in reagent_spc.iterrows():
        if reag_chem not in reagent_list:
            reagent_list.append(reag_chem)
        else:
            pass
    count=0
    for reagent in reagent_list:
        count+=1
        for reag_chem, row in reagent_spc.iterrows():
            if reagent in reag_chem:
                chemical_list=[]
                chemical_list.append(reagent_spc.at[reag_chem, 'name'])
                chemical_list.append(reagent_spc.at[reag_chem, 'InChIKey'])
                chemical_list.append(reagent_spc.at[reag_chem, 'density'])
                chemical_list.append(reagent_spc.at[reag_chem, 'm_type'])
                chemical_list.append(reagent_spc.at[reag_chem, 'molecular mass'])
                chemical_list.append(reagent_spc.at[reag_chem, 'amount'])
        onereagent_df=(pd.DataFrame(chemical_list).transpose())
        onereagent_df.columns = ['name','InChiKey', 'density', 'm_type','molecularmass','amount']
        conc_cells = (ReagentConc(onereagent_df, reagent))
        if conc_cells == 'null':
            pass
        else:
            conc_cells_df=pd.DataFrame(conc_cells)
            conc_df=pd.concat([conc_df,conc_cells_df], axis=1)
    return(conc_df)

#parases each index of the json file and returns a normalized data frame with each experiment (well) containing all relevant information
def flatten_json_reg(y):
    out = {}
    def flatten(x, name=''):
        #Flattens the dictionary type to a single column and entry
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        #Flattens list type to a single column and entry
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            ## Parses the namespace if the units are contained with the value from the JSON
            if ':' in str(x):
                x1,x2=x.split(":")
                out[str('_raw_reagent_' + name[:-1])] = x1 #Drops to the default namespace 
                out[str('_raw_reagent_' + name[:-1] + '_units')] = str(x2) # adds a new entry for the column with the units which is adjacent to the measurement and otherwise has an identical name
            else:
                out[str('_raw_reagent_' + name[:-1])] = str(x) #If the JSON value was a not a unit based value, the key value pair is returned as a column header and entry
    flatten(y) # Flattens the rest of the formating for pandas import
    return(pd.io.json.json_normalize(out)) # normalizes the data and reads into a single row data frame

def reag_info(reagentdf,chemdf):
    ## ignore GBL ##
    reagentlist=[]
    for item in list(reagentdf):
        name='null'
        mm='null'
        density='null'
        if ('_raw_reagent_' in item) and ('InChIKey' in item):
            for InChIKey in reagentdf[item]:
                m_type='null'
                parse_name=item[:-21]
                parse_chemical_name=item[:-9]
        ### Eventually this section of code can be replaced by class objects which describe the reagent for each experiment in line
        ### Currenlty this code is assembling a dataframe which describes the relatinship between various reagents in order
        ### to perform subsequent calcualtions.  The inchi keys for example are hard coded and should be variable
                #FA
                if InChIKey == "BDAGIHXWWSANSR-UHFFFAOYSA-N":
                    mm=(float(chemdf.loc[InChIKey,"Molecular Weight (g/mol)"]))# / float(chemdf.loc["FAH","Density            (g/mL)"])
                    name=((chemdf.loc[InChIKey,"Chemical Name"]))# / float(chemdf.loc["FAH","Density            (g/mL)"])
                    m_type='pureliquid' #Assigns an object type for later analysis and calculation of concentrations
                    try:
                        density=(float(chemdf.loc[InChIKey,"Density            (g/mL)"]))# / float(chemdf.loc["FAH","Density            (g/mL)"])
                    except:
                        print("Error with the configuration of reference sheet.  Abort run and check formic acid details in google sheets (2018-08-11)")
                        break
                #GBL
                elif InChIKey =='YEJRWHAVMIAJKC-UHFFFAOYSA-N': 
                    mm=(float(chemdf.loc[InChIKey,"Molecular Weight (g/mol)"]))# / float(chemdf.loc["FAH","Density            (g/mL)"])
                    name=((chemdf.loc[InChIKey,"Chemical Name"]))# / float(chemdf.loc["FAH","Density            (g/mL)"])
                    m_type='solvent'
                    try:
                        density=(float(chemdf.loc[InChIKey,"Density            (g/mL)"]))# / float(chemdf.loc["FAH","Density            (g/mL)"])
                    except:
                        pass
                #PbI2
                elif InChIKey == 'RQQRAHKHDFPBMC-UHFFFAOYSA-L':
                    mm=(float(chemdf.loc[InChIKey,"Molecular Weight (g/mol)"]))# / float(chemdf.loc["FAH","Density            (g/mL)"])
                    name=((chemdf.loc[InChIKey,"Chemical Name"]))# / float(chemdf.loc["FAH","Density            (g/mL)"])
                    m_type='inorg'
                #Null
                elif InChIKey == 'null':
                    pass
                #Organic
                else:
                    mm=(float(chemdf.loc[InChIKey,"Molecular Weight (g/mol)"]))# / float(chemdf.loc["FAH","Density            (g/mL)"])
                    name=((chemdf.loc[InChIKey,"Chemical Name"]))# / float(chemdf.loc["FAH","Density            (g/mL)"])
                    m_type='org'
                    try:
                        density=(float(chemdf.loc[InChIKey,"Density            (g/mL)"]))# / float(chemdf.loc["FAH","Density            (g/mL)"])
                    except:
                        pass
                for (item) in list(reagentdf):
                    if (parse_chemical_name in item) and ("actual_amount" in item) and not ("units" in item):
                        amount=(reagentdf.loc[0,item])
            reagentlist.append((name, mm, density, parse_name, InChIKey, m_type, amount))
    reagentlist_df=(pd.DataFrame(reagentlist, columns=['name', 'molecular mass', 'density', 'parsed name', "InChIKey", "m_type", "amount"]))
    return(reagentlist_df)

def reagentparser(firstlevel, myjson, chem_df):
    for reg_key,reg_value in firstlevel.items():
        if reg_key == 'reagent':
            reagent_df=flatten_json_reg(reg_value)
            reagent_spec=reag_info(reagent_df,chem_df) #takes all of the infomration from the chem_df (online web information here) and puts it together with the the reagent information
            Conc_df=calcConc(reagent_df, reagent_spec) #takes the information abot the reagents and generates ... 

        if reg_key == 'run':
            run_df=flatten_json(reg_value)
        if reg_key == 'tray_environment':
            tray_df=dict_listoflists(reg_value)
         ##### Currently ommitting this line of data as the information here does not add detail to the data structure ### 
#        if reg_key == 'robot_reagent_handling':
#            robo_df=robo_handling(reg_value)
#                print(item)
        ########### See above note, still should parse later!!! after v1.1 ###  
        ###The following column headers are only approxpriate for workflow 1.1, if the namespace changes these lines will need to be reformmated 
        ### rxn reagent_4 and 5 assume that the input chemicals are formic acid, the volume of which can be learned on directly 
        ### this assumtion might not be valid in all future cases
        if reg_key == 'well_volumes':
            well_volumes_df=pd.DataFrame(reg_value, columns=['_raw_vialsite', '_raw_reagent_0_volume', '_raw_reagent_1_volume', '_raw_reagent_2_volume', '_raw_reagent_3_volume', '_raw_reagent_4_volume', '_raw_reagent_5_volume', '_raw_labwareID'])
        if reg_key == 'crys_file_data':
            crys_file_data_df=pd.DataFrame(reg_value, columns=['_raw_vialsite', '_out_crystalscore'])
    #The following code aligns and normalizes the data frames
    experiment_df=well_volumes_df.merge(crys_file_data_df)
    wellcount=(len(experiment_df.index))-1
    fullrun_df=(run_df.append([run_df]*wellcount,ignore_index=True))
    fullConc_df=(Conc_df.append([Conc_df]*wellcount,ignore_index=True))
    fullreagent_df=(reagent_df.append([reagent_df]*wellcount,ignore_index=True))
    fulltray_df=(tray_df.append([tray_df]*wellcount, ignore_index=True))
    out_df=pd.concat([fullConc_df, experiment_df, fullreagent_df, fullrun_df, fulltray_df], axis=1)
    return(out_df)