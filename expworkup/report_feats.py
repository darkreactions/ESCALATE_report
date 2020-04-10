#Copyright (c) 2020 Ian Pendleton - MIT License

import logging
import pandas as pd
from chemdescriptor.generator.chemaxon import ChemAxonDescriptorGenerator as cag

modlog = logging.getLogger('report.feats')

def get_command_dict(command_type_df, one_type, application):
    """Converts expworkup.type_command.csv to dict for chemdescriptor

    Parameters
    ----------
    command_type_df : pd.DataFrame generated from type_command.csv

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
    commands_df = command_type_df[(command_type_df['types'] == one_type) &  \
                                  (command_type_df['actor_systemtool_name'] == application)]

    descriptor_dict = {}
    for command in commands_df.itertuples():
            column_name = f'_feat_{command.short_name}'
            descriptor_dict[command.short_name] = {}
            descriptor_dict[command.short_name]["command"] = command.calc_definition.split(' ')
            descriptor_dict[command.short_name]["column_names"] = [column_name]
    command_dict = {}
    command_dict['descriptors'] = descriptor_dict
    command_dict['ph_descriptors'] = {} # possibly useful, see chemdescriptor for more details
    if len(command_dict['descriptors'].keys()) == 0:
        return None
    else:
        return(command_dict)

def get_features(unique_types, experiment_inchi_df, target_name, log_folder):
    """ gather specified features to describe all expUID+inchi+type combinations
    All features for experimentUID + InchiKey + types combinations across
    labs are gathered and returned as a dict of pd.Dataframes 

    Parameters
    ----------
    unique_types : set of chemical_types for the full dataset

    experiment_inchi_df : pd.Dataframe with [expUID, inchikeys, types]
        which contains all unique combinations of expUID+inchikeys 
        along with associated type list for each

    target_name : name of the working data directory
        used for report logs 

    log_folder : folder location to stores logs 
    
    Returns
    -------
    type_feat_dict : dict of pd.DataFrames containing features
        keys:one_type label, values:dfs of features from chemdescriptor

    """
    #TODO: fix logging issues
    modlog.info(f'Generating physicochemical features to {target_name} dataset')
    print(f'Generating physicochemical features to {target_name} dataset')

    # TODO: insert validation of the type_command df
    # type_command df should be treated as code!
    type_command_df = pd.read_csv('./expworkup/type_command.csv')

    type_feat_dict = {}
    for one_type in unique_types:
        #only grabs EXACT string matches for the type
        correct_type =\
            experiment_inchi_df[experiment_inchi_df['types']\
            .str.contains(pat=f'(?:^|\W){one_type}(?:$|\W)',\
                          regex=True)]
        #gather information we will want in the final dataframe for features
        inchi_smiles = correct_type[['inchikeys', 'smiles', 'types']]\
                                   .drop_duplicates(keep="first", 
                                                    ignore_index=True)
        smiles_list = inchi_smiles['smiles'].values.tolist()
        type_command_dict = get_command_dict(type_command_df,
                                             one_type,
                                             'cxcalc')
        if type_command_dict is not None:
            #TODO: update logs and fix outputfile meaningfully
            features = cag(smiles_list,
                           whitelist={},
                           command_dict=type_command_dict,
                           logfile=f'{log_folder}/CXCALC_LOG.txt',
                           standardize=True)
            type_features_df = features.generate('output.csv',
                                                 dataframe=True,
                                                 lec=False)
            type_feat_dict[one_type] =  pd.concat([inchi_smiles, type_features_df], axis=1)
            type_feat_dict[one_type].rename(columns={"Compound": "smiles_standardize"}, inplace=True)

    modlog.info(f'Appending physicochemical features to {target_name} dataframe')
    print(f'Appending physicochemical features to {target_name} dataframe')
    return(type_feat_dict)

def unpack_features(type_feat_dict):
    """ Digest dictionary of features and convert to enumerated dataframe

    Parameters
    ----------
    type_feat_dict : dict of pd.DataFrames containing features
        keys:one_type label, values:dfs of features from chemdescriptor
    
    Returns
    ----------
    inchi_key_indexed_features_df : pd.DataFrame with all features
        features are indexed on the inchikeys and contain columns
        associated with all computed features. Empty cells are returned
        for intersections with no values.  

    Notes
    ----------
    * Inchikeys that are associated with multiple chemical_types will merge
    to a single row (merge down) see notes on pd.DataFrame.combine_first()
    for more information
    """
    inchi_key_indexed_features_df = pd.DataFrame()
    for one_type in type_feat_dict:
        df = type_feat_dict[one_type].set_index('inchikeys')

        # Merge the dataset into the existing index DOES NOT OVERWRITE
        inchi_key_indexed_features_df = \
            inchi_key_indexed_features_df.combine_first(df)
    return(inchi_key_indexed_features_df)

def get_chemical_types(inchi, lab, chem_df_dict):
    """ Retrieve lab specified chemical types for a given inchi key

    Parameters
    ----------
    inchi : inchikey to be used in lookup

    lab : target lab to read types

    chemdf_dict : dict of pandas.DataFrames assembled from all lab inventories
        reads in all of the chemical inventories which describe the chemical content
        from each lab used across the dataset construction 
    
    Returns
    ----------
    pd.Series(<list of types>, <smiles string associated with inchikey>)

    Notes
    ----------
    * TODO: add more 'human nonsense' removal / general string cleaning
    """
    smiles = chem_df_dict[lab].loc[inchi, 'Canonical SMILES String']

    # Convert all to lowercase entries for string matching
    types = chem_df_dict[lab].loc[inchi, 'Chemical Category'].strip(' ').lower()
    #clean the list before returning
    types_list = [x.strip(' ') for x in types.split(',')]
    return(pd.Series((types_list, smiles)))

def feat_pipeline(target_name, report_df, chem_df_dict, debug, log_folder):
    """ Manage ingest of lab content and return feature dataframes

    Parameters
    ----------
    target_name : name of the target folder (and final curated csv output)

    report_df : pandas.DataFrame
        dataframe returned after parsing all content from google drive
        returned from expworkup.json_pipeline

    chemdf_dict : dict of pandas.DataFrames assembled from all lab inventories
        reads in all of the chemical inventories which describe the chemical content
        from each lab used across the dataset construction

    debug_bool : CLI argument, True=Enable debugging

    log_folder : folder location to stores logs

    Returns
    -------
    ()
    
    """
    report_copy = report_df.set_index('name')

    # Build a dataframe of every unique inchikey
    inchi_df = report_copy.filter(regex='reagent_._chemicals_._inchikey')
    inchi_df = inchi_df.dropna(how='all', axis=1)
    inchi_df.fillna(value='null', inplace=True)
    unique_inchis_series = inchi_df.apply(lambda x: list(set(x)), axis = 1)
    unique_inchis_df = unique_inchis_series.to_frame(name='inchikeys')
    unique_inchis_df['lab'] = report_copy['_raw_lab']
    unique_inchis_df.reset_index(inplace=True)

    # creates a stacked dataframe with each runUID and inchikey combination as a unique row
    expanded_unique_inchis_df = unique_inchis_df.explode('inchikeys')
    expanded_unique_inchis_df['name'] = expanded_unique_inchis_df.index
    expanded_unique_inchis_df = expanded_unique_inchis_df[expanded_unique_inchis_df['inchikeys'] != 'null']

    # add chemicals_types ('types') and the associated smiles based on inchikey
    # requires that the correct lab is provided (the lab is identified in the runUID)
    expanded_unique_inchis_df[['types','smiles']] = \
        expanded_unique_inchis_df.apply(lambda x: get_chemical_types(x['inchikeys'], 
                                                                     x['lab'],
                                                                     chem_df_dict),
                                                                            axis=1)
    # a bit of ugly to remove the list structure in types column
    expanded_unique_inchis_df['types'] = \
        [','.join(map(str, l)) for l in expanded_unique_inchis_df['types']]
    # Get only the unique instances of each chemical types in the dataset
    ugh_list = list(entry for entry in expanded_unique_inchis_df['types'].values.tolist())
    #Unpack potential list of lists and return uniques
    unique_types = list(set([x for l in ugh_list for x in l.split(',')]))
    unique_types.sort()
    type_feat_dict = get_features(unique_types,
                                  expanded_unique_inchis_df,
                                  target_name,
                                  log_folder)


    ###### Dataframe Export Begins Here ####### 
    #TODO: add the garysmod export option

    ## Output dataframe with association between unique inchikeys and features
    inchi_key_indexed_features_df = unpack_features(type_feat_dict)
    inchi_key_indexed_features_df.groupby("inchikeys").ffill()\
                                 .groupby("inchikeys").last()\
                                 .reset_index(inplace=True)
    inchi_key_indexed_features_df.to_csv('perov_desc.csv')

    ## Output dataframe with runID x inchi column(s)
    # rename list of inchis to a column / inchi 
    tags = unique_inchis_df['inchikeys'].apply(pd.Series)
    tags = tags.rename(columns = lambda x : 'inchikey_' + str(x))

    tags['name'] = unique_inchis_df['name']
    tags.set_index('name', inplace=True)
    tags.to_csv('runUID_inchi_loadtable.csv')

    """
    export a load table with the set of all the inchikeys associated with a dataframe
    export a type table which can be used to downselect to particular reagents/inchis
    export all features indexed on the set of inchikeys
    """

#    return feat_df