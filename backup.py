def _prepare(shuffle=0, deep_shuffle=0):
    ''' reads in perovskite dataframe and returns only experiments that meet specific criteria

    --> Data preparation occurs here
    criteria for main dataset include experiment version 1.1 (workflow 1 second generation), only
    reactions that use GBL, and 
    '''
    perov = pd.read_csv(os.path.join(VERSION_DATA_PATH, CRANK_FILE), skiprows=4, low_memory=False)
    perov = perov[perov['_raw_ExpVer'] == 1.1].reset_index(drop=True)

    # only reaction that use GBL as a solvent (1:1 comparisons -- DMF and other solvents could convolute analysis)    
    perov = perov[perov['_raw_reagent_0_chemicals_0_InChIKey'] == "YEJRWHAVMIAJKC-UHFFFAOYSA-N"].reset_index(drop=True)    

    # removes some anomalous entries with dimethyl ammonium still listed as the organic.
    perov = perov[perov['_rxn_organic-inchikey'] != 'JMXLWMIFDJCGBV-UHFFFAOYSA-N'].reset_index(drop=True)

    #We need to know which reactions have no succes and which have some
    organickeys = perov['_rxn_organic-inchikey']
    uniquekeys = organickeys.unique()

    df_key_dict = {}
    #find an remove all organics with no successes (See SI for reasoning)
    for key in uniquekeys:
        #build a dataframe name by the first 10 characters of the inchi containing all rows with that inchi
        df_key_dict[str(key)] = perov[perov['_rxn_organic-inchikey'] == key]
    all_groups = []
    successful_groups = []
    failed_groups = []
    for key, value in df_key_dict.items():
        all_groups.append(key)
        if 4 in value['_out_crystalscore'].values.astype(int):
            successful_groups.append(key)
        else:
            failed_groups.append(key)

    #only grab reactions where there were some recorded successes in the amine grouping
    successful_perov = (perov[perov['_rxn_organic-inchikey'].isin(successful_groups)])
    successful_perov = successful_perov[successful_perov['_rxn_organic-inchikey'] != 'JMXLWMIFDJCGBV-UHFFFAOYSA-N'].reset_index(drop=True)

    # we need to do this so we can drop nans and such while keeping the data consistent
    # we couldnt do this on the full perov data since dropna() would nuke the entire dataset (rip)
    all_columns = successful_perov.columns
    
    full_data = successful_perov[all_columns].reset_index(drop=True)

    full_data = full_data.fillna(0).reset_index(drop=True)
    successful_perov = full_data[full_data['_out_crystalscore'] != 0].reset_index(drop=True)
    
    ## Shuffle options for these unique runs
    out_hold = pd.DataFrame()
    out_hold['out_crystalscore'] = successful_perov['_out_crystalscore']
    if shuffle == 1:
        out_hold['out_crystalscore'] = successful_perov['_out_crystalscore']
        successful_perov = successful_perov.reindex(np.random.permutation(successful_perov.index)).reset_index(drop=True)
        successful_perov['_out_crystalscore'] = out_hold['out_crystalscore']
    if deep_shuffle == 1:
        # Only want to shuffle particular columns (some shuffles will break processing), we will attempt to describe each selection in text

        #build holdout (not shuffled)
        out_hold_deep_df = pd.DataFrame()
        out_hold_deep_df = successful_perov.loc[:, '_raw_model_predicted':'_prototype_heteroatomINT']
        out_hold_deep_df = pd.concat([successful_perov['_rxn_organic-inchikey'], out_hold_deep_df], axis=1) 

        #isolate shuffle set
        shuffle_deep_df = pd.DataFrame()
        shuffle_deep_df = pd.concat([successful_perov.loc[:, 'name':'_rxn_M_organic'], 
                                     successful_perov.loc[:, '_rxn_temperatureC_actual_bulk' : '_feat_Hacceptorcount']], 
                                     axis = 1)
        successful_perov = shuffle_deep_df.apply(np.random.permutation)

        successful_perov.reset_index(drop=True)
        successful_perov = pd.concat([out_hold_deep_df, successful_perov], axis=1)

    successful_perov.rename(columns={"_raw_v0-M_acid": "_rxn_v0-M_acid", "_raw_v0-M_inorganic": "_rxn_v0-M_inorganic", "_raw_v0-M_organic":"_rxn_v0-M_organic"}, inplace=True)

    return successful_perov

def molarity_calc(raw_df, finalvol_entries):
    """
    calculate the molarity for each experiment

    :param raw_df:  the whole dataframe of all raw values
    :param finalvol_entries: final volume columns from the raw dataframe
    :return: chemical molarity dataframe for each experiment
    """
    ##Calculate Reagent volumes
    reagent_list=[]
    for header in raw_df.columns:
        if "volume" in header:
            reagent_list.append(header)
    total_list=reagent_list[:-finalvol_entries]
    df_total_list=raw_df[total_list]
    total_vol_df = df_total_list.sum(axis=1)
    df_final_vols=raw_df[reagent_list]
    final_vol_df = df_final_vols.sum(axis=1)
    calculated_volumes_df=pd.concat([total_vol_df, final_vol_df],axis=1)
    calculated_volumes_df.columns=['_raw_total_volume', '_raw_final_volume']
    ## Calculate molarity (grab reagent mmols and then use the volumes caclualted above to detrmine the "nomial molarity")
    mmol_reagent_list=[]
    for header in raw_df.columns:
        if '_raw_mmol_' in header and 'final' in header:
            mmol_reagent_list.append(header)
    mmol_reagent_df = raw_df[mmol_reagent_list]    
    molarity_df = pd.DataFrame()
    for header in mmol_reagent_df:
        newheader='_raw_M_'+header[10:-6]+'_final'
        molarity_df[newheader] = mmol_reagent_df[header] / (calculated_volumes_df['_raw_final_volume']/1000)

    mmol_reagent_list_2=[]
    for header in raw_df.columns:
        if '_raw_v1-mmol_' in header and 'final' in header:
            mmol_reagent_list_2.append(header)
    mmol_reagent_df_2 = raw_df[mmol_reagent_list_2]
    molarity_df_2 = pd.DataFrame()
    for header in mmol_reagent_df_2:
        newheader2 = '_raw_v1-M_' + header[13:-6]+'_final'
        molarity_df_2[newheader2] = mmol_reagent_df_2[header] / (calculated_volumes_df['_raw_final_volume']/1000)
    molarity_df = pd.concat([molarity_df, molarity_df_2], axis=1)
    return(molarity_df)


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



def calcConc(reagentdf, reagent_spc):
    """ calculates concentration of reagents

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

def combine(portioned_df):
    combined_df=pd.DataFrame()
    reagentlist=[]
    mmol_df_dict={}
    for header in list(portioned_df):
        # prevents trying to sum a single item and instead just adds the series back to a dataframe for later use
        if len(list(portioned_df[header].shape)) == 1:
            header_df_name=header+"_final"
            temp_df=pd.DataFrame(portioned_df[header])
            temp_df.columns=[header_df_name]
            mmol_df_dict[header]=temp_df
        else: 
            if header in reagentlist:
                pass
        #Takes all dataframe columns with name "header" and sums the values together returning the summed list to a header referenced dict
        #Probably a better way to do this, but it works for all dataframes in the test set
            else:
                mmol_value=[]
                for index, experiment in portioned_df[header].iterrows():
                    mmol_list=[]
                    for item in experiment:
                        mmol_list.append(item)
                    mmol_value.append((sum(mmol_list)))
                header_df_name=header+"_final"
                mmol_df_dict[header]=pd.DataFrame(mmol_value, columns=[header_df_name])
                reagentlist.append(header)
    #outputs all of the dataframes created during the mmol summing process and returns with new header for later
    for (k,v) in mmol_df_dict.items():
        combined_df=pd.concat([combined_df, v], axis=1)
    return(combined_df)

def calc_mmol_v1(vol, index, reagent_name, JsonParsed_df):
    """
    calculates the mmol of the reagent and returns to a list (v1 using new concentration model)

    order is important as the indexes are not maintained through this step)
    The calculation is done indpendently to better handle different chemicals
    (and thereby be more flexible moving forward) using the index



    :param vol:
    :param index:
    :param reagent_name:
    :param JsonParsed_df:
    :return:
    """
    #    print(vol, index, reagent_name)
    mmol_cell={}
    for header in list(JsonParsed_df):
        if ('_v1-conc_' in header) and (reagent_name in header):
            mmol_name=('_raw_v1-mmol_' + header[23:])
            #            print(((vol*JsonParsed_df.loc[index, header]/1000)), header)
            mmol_cell[mmol_name]=((vol*JsonParsed_df.loc[index, header]/1000))
    #Returns a dictionary with the key set to the _raw_mmol + inchi string taken from the parsed mmol name above.
    return(mmol_cell)

def calc_mmol(vol, index, reagent_name, JsonParsed_df):
    """
    calculates the mmol of the reagent and returns to a list

    order is important as the indexes are not maintained through this step)
    The calculation is done indpendently to better handle different chemicals
    (and thereby be more flexible moving forward) using the index



    :param vol:
    :param index:
    :param reagent_name:
    :param JsonParsed_df:
    :return:
    """
#    print(vol, index, reagent_name)
    mmol_cell={}
    for header in list(JsonParsed_df):
        if ('_conc_' in header) and (reagent_name in header):
            mmol_name=('_raw_mmol_' + header[20:])
#            print(((vol*JsonParsed_df.loc[index, header]/1000)), header)
            mmol_cell[mmol_name]=((vol*JsonParsed_df.loc[index, header]/1000))
    #Returns a dictionary with the key set to the _raw_mmol + inchi string taken from the parsed mmol name above. 
    return(mmol_cell)

def volcheck(vol_series, reagent_name, JsonParsed_df,runID_df):
    """
    Converts volumes of reagents into mmol of chemicals

    :param vol_series: series of volumes from a reagent in a given experiment
    :param reagent_name: reagent under consideration
    :param JsonParsed_df: full json file from the parsed json (likely overkill and can be reduced)
    :param runID_df: runID of the given set of experiments
    :return: dataframe of calculated mmol from v0 concentration and v1 concentration
    """
    index=0
    mmol_index_list=[]
    mmol_index_list_v1=[]
    for volume in vol_series:
            runID=runID_df.loc[index,'RunID_vial']
            mmol_index_list.append(calc_mmol(volume, index, reagent_name, JsonParsed_df))
            mmol_index_list_v1.append(calc_mmol_v1(volume, index, reagent_name, JsonParsed_df))
            index+=1
    mmol_df_out=pd.DataFrame(mmol_index_list)
    mmol_index_list_v1 = pd.DataFrame(mmol_index_list_v1)
    if mmol_df_out.size == 0:
        pass
    else:
        mmol_df_out = pd.concat([mmol_df_out, mmol_index_list_v1], axis=1)
        return(mmol_df_out)

#splits of the reagents to analyze the volumes of the important reagents for the current workflows (wkflow1.1 is 2,3,4)
def mmol_breakoff(JsonParsed_df, runID_df):

    out_df = pd.DataFrame()
    reagent_mmol_df=pd.DataFrame()

    for columnname in list(JsonParsed_df):
        if ("_raw_reagent_" in columnname) and ("_volume" in columnname):
            if columnname == "_raw_reagent_0_volume":
                pass
            else:
                reagent_name=columnname[:-6]
                reagent_mmol_df = volcheck(JsonParsed_df[columnname], reagent_name, JsonParsed_df, runID_df)

            out_df=pd.concat([out_df, reagent_mmol_df], axis=1, sort=False)

    out_combined_df=combine(out_df)  
    return(out_combined_df)

# Send out each row of the ingretients as a lambda, parse by reagent, build class objects, return to new df with compound ingredient objects
    


    #reagent_spec = reag_info(reagent_df,chem_df) #takes all of the infomration from the chem_df (online web information here) and puts it together with the the reagent information
    #concentration_df = calcConc(reagent_df, reagent_spec) #takes the information abot the reagents and generates ... 

    #reagent_objects_df = curate_reagent_objects(reagent_details_df, nominal=False, export_observables=export_observables)
    #build smarts for object distinction here
    #representative_tray_uids = entity.get_tray_uids(report_df)
    #modlog.info(f'Generated compound ingredient (reagents) objects in {reagent_objects_file}')


    #build smarts for model distinction here
    #reagent_details_nominal_df = get_reagent_df(report_df, nominal=False)# default is to return the actuals (nominals can be toggled)
    #reagent_nominal_file = export_reagent_objects(perovskite_df, target_naming_scheme, nominal=True)

    #modlog.info(f'Generated compound ingredient (reagents) nominals in {reagent_nominal_file}')
    
    # Setup the chemical dataframe for reading out chemical names (specific for 1.1)
    # TODO: generalize beyond 1.1 for direct reaction reproductions from reagent objects --> to models (harder, hypothesis)

    #conc_dict_out = {}
    #for exp_uid, row in conc_df.iterrows():
    #    conc_dict_out[exp_uid] = {}
    #    chem_df = chemdf_dict[get_experimental_run_lab(exp_uid)]
    #    chem_df = chem_df.fillna('null') #insert our choice placeholder for blank values --> 'null'
    #    conc_dict_out[exp_uid]['chemical_info'] = {}
    #    conc_dict_out[exp_uid]['chemical_info']['solvent'] = (conc_df.loc[exp_uid,'_raw_reagent_0_chemicals_0_InChIKey'], 
    #                                                        chem_df.loc[conc_df.loc[exp_uid,'_raw_reagent_0_chemicals_0_InChIKey'],"Chemical Name"]
    #                                                        ) 
    #    conc_dict_out[exp_uid]['chemical_info']['inorganic'] = (conc_df.loc[exp_uid,'_raw_reagent_1_chemicals_0_InChIKey'], 
    #                                                        chem_df.loc[conc_df.loc[exp_uid,'_raw_reagent_1_chemicals_0_InChIKey'],"Chemical Name"],
    #                                                        conc_df.loc[exp_uid,'_raw_reagent_1_chemicals_0_v1conc']                                                               
    #                                                        ) 
    #    conc_dict_out[exp_uid]['chemical_info']['organic-1'] = (conc_df.loc[exp_uid,'_raw_reagent_1_chemicals_1_InChIKey'], 
    #                                                        chem_df.loc[conc_df.loc[exp_uid,'_raw_reagent_1_chemicals_1_InChIKey'],"Chemical Name"],
    #                                                        conc_df.loc[exp_uid,'_raw_reagent_1_chemicals_1_v1conc']                                                               
    #                                                        ) 
    #    conc_dict_out[exp_uid]['chemical_info']['organic-2'] = (conc_df.loc[exp_uid,'_raw_reagent_2_chemicals_0_InChIKey'], 
    #                                                        chem_df.loc[conc_df.loc[exp_uid,'_raw_reagent_2_chemicals_0_InChIKey'],"Chemical Name"],
    #                                                        conc_df.loc[exp_uid,'_raw_reagent_2_chemicals_0_v1conc']                                                               
    #                                                        ) 
    #    conc_dict_out[exp_uid]['organic_inchi'] = conc_df.loc[exp_uid, '_rxn_organic-inchikey']


def build_conc_df(df):
    '''
    Takes in perovskite dataframe and returns a list of "first runs" for a given set of reagents.  
    These 'first runs' are often representative of the maximum solubility limits of a given space.  
    Likely there will be some strangeness in the first iteration that requires tuning of these filters.

    Assumptions: 
    1. ommitting reagents 4-5 for "uniqueness" comparison
    2. need to generalize to use experimental volumes to determing which experiments use which reagents
    3. definitely is missing some of the unique reagents (some are performed at various concetnrations, not just hte first)
    
    Filters are explained in code
    '''
    # Only consider runs where the workflow are equal to 1.1 (the ITC method after initial development)
    # TODO: generalize beyond 1.1
    # #remove this once testing is complete and the reagent nominals / objects are exportable
    df = df[df['_raw_ExpVer'] == 1.1].reset_index(drop=True) # Harded coded to 1.1 for development

    # removes some anomalous entries with dimethyl ammonium still listed as the organic.
    #perov = perov[perov['_rxn_organic-inchikey'] != 'JMXLWMIFDJCGBV-UHFFFAOYSA-N'].reset_index(drop=True)

    # build a list of all unique combinations of inchi keys for the remaining dataset
    experiment_inchis_df = df.filter(regex='_raw_reagent.*.InChIKey')
    experiment_inchis = (experiment_inchis_df.columns.values)
    experiment_inchis_df = pd.concat([df['name'], experiment_inchis_df], axis=1)
    ignore_list = ['name']

    df.set_index('name', inplace=True)

    # clean up blanks, and bad entries for inchikeys, return date sorted df
    experiment_inchis_df.replace(0.0, np.nan, inplace=True)
    experiment_inchis_df.replace(str(0), np.nan, inplace=True)
    experiment_inchis_df.set_index('name', inplace=True)
    experiment_inchis_df.sort_index(axis=0, inplace=True)

    # find extra reagents (not present in any initial testing runs) (i.e. experiments with zeroindex-reagents 2 < x > 5)
    secondary_reagent_columns = []
    for column in experiment_inchis_df.columns:
        if column in ignore_list:
            pass
        else:
            reagent_num = int(column.split("_")[3]) # 3 is the value of the split for reagent num
            if reagent_num > 10 and reagent_num < 11:
                secondary_reagent_columns.append(column)
    experiment_inchis_df.drop(secondary_reagent_columns, axis=1, inplace=True) 
    experiment_inchis_df.drop_duplicates(inplace=True)
#    experiment_inchis_df = pd.concat([df, experiment_inchis_df], axis=1, join='inner')
    experiment_inchis_df['name'] = experiment_inchis_df.index

    # get the concentration of the relevant associated inchi keys for all included reageagents

    def get_chemical_conc(reagent, inchi, index):
        reagent_inorg_header = f'_raw_reagent_{reagent}_v1-conc_{inchi}'
        try:
            inorg_conc = df.loc[index, reagent_inorg_header]
        except:
            inorg_conc = 0
        return inorg_conc
    
    prototype_df = experiment_inchis_df.copy()
    for column in experiment_inchis_df.columns:
        if column in ignore_list:
            pass
        else:
            column_name_split = column.split("_")
            new_column_name = '_'.join(column_name_split[:6]) + '_v1conc'
            reagent_num = column.split("_")[3]
            prototype_df[new_column_name] = experiment_inchis_df.apply(lambda x: get_chemical_conc(reagent_num, x[column], x['name']), axis=1)
    prototype_df = pd.concat([df['_rxn_organic-inchikey'], prototype_df], axis=1, join='inner')
    return prototype_df