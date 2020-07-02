import pandas as pd

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
    # Funky return allows parsing by the lambda caller
    return(pd.Series((types_list, smiles)))

def get_unique_chemicals_types_byinstance(molarity_df_type_pivot):
    """ Unique handler to gather unique primary one_type instances into individual columns
      molarity column and inchi column as list of lists

    TODO: add to schemas.py
    Parameters
    ----------

        main_type                                           acid
        name               inchikey                             
        2017-10-1 ... L_A1 BDAGIHXWWSANSR-UHFFFAOYSA-N  2.923979 ... 
                           RQQRAHKHDFPBMC-UHFFFAOYSA-L       NaN
                           UPHCENSIMPJEIS-UHFFFAOYSA-N       NaN
                           YEJRWHAVMIAJKC-UHFFFAOYSA-N       NaN

    Returns
    -------
    unique_chemicals_types_byinstance : pd. DataFrame
        include the '_molarity' of each unique chemical type in a given run
        along with the inchikey of the particular chemical. 
        Headers are consistent
        Ex.
                          acid_0  ... acid_inchikey_<N>
        name                                                              
        2017-10 ... _A1  2.923979 ... BDAGIHXWWSANSR-UHFFFAOYSA-N
        2017-10 ... _B1  4.779492 ... BDAGIHXWWSANSR-UHFFFAOYSA-N
        2017-10 ... _C1  4.779492 ... BDAGIHXWWSANSR-UHFFFAOYSA-N

    """
    chemicals_types_bundle_df = \
        molarity_df_type_pivot.groupby(level=[0]).apply(lambda x: chemical_type_sorting(x))
    chemicals_types_bundle_df = chemicals_types_bundle_df.droplevel(1) #remove extra runUID column

    #unapck the bundle 
    unique_chemicals_types_byinstance = \
        pd.DataFrame(index=chemicals_types_bundle_df.index)
    for column in chemicals_types_bundle_df.columns:
        mynew_df = expand_columns(chemicals_types_bundle_df, column)
        unique_chemicals_types_byinstance = \
            unique_chemicals_types_byinstance.join(mynew_df, how='left')
    inchi_columns = [x for x in unique_chemicals_types_byinstance.columns if 'inchikey' in x]
    molarity_columns = [x for x in unique_chemicals_types_byinstance.columns if 'inchikey' not in x]
    for column in inchi_columns:
        fixed_column = '_'.join([column.split('_')[0], column.split('_')[2], 'inchikey']) 
        unique_chemicals_types_byinstance.rename(columns = {column:fixed_column}, inplace = True)
    for column in molarity_columns:
        fixed_column = f'{column}_molarity'
        unique_chemicals_types_byinstance.rename(columns = {column:fixed_column}, inplace = True)
    return unique_chemicals_types_byinstance

def expand_columns(df, column):
    """ expands specified column with lists into multiple columns

    Parameters
    ----------
    df : pd.DataFrame, indexed on runUID
        columns could be potentailly a nested list of values
    
    column : target column name for unpacking

    Returns
    -------
    df : unpacked nested columns

    """
    df = pd.DataFrame(df[column].values.tolist(), df.index).add_prefix(f'{column}_')
    return df

def chemical_type_sorting(x):
    """ parse the lines of the molarity table to compress type values to a single column of lists

    Parameters
    ----------
    x : single row of the molarity table. See example structure, chemical type columns dictated
        the number of assignments in the chemical inventories

        main_type                                                                acid 
        name                                    inchikey                              
        2017-10-18T19_58_20.000000+00_00_LBL_A1 BDAGIHXWWSANSR-UHFFFAOYSA-N  2.923979 ...
                                                RQQRAHKHDFPBMC-UHFFFAOYSA-L       NaN 
                                                UPHCENSIMPJEIS-UHFFFAOYSA-N       NaN 
                                                YEJRWHAVMIAJKC-UHFFFAOYSA-N       NaN 

    Returns
    -------
    sorted_df : pd.DataFrame, structure shown
                                                            acid       acid_inchikey
    2017-10-18T19_58_20.000000+00_00_LBL_A1  [2.9239788688021044] ...  BDAGIHXWWSANSR-UHFFFAOYSA-N] ... 
    """
    molarity_list = []
    temp_id = []
    for column in x.columns:
        raw_M_list = x[column].values.tolist() #e.g. [nan, nan, nan, 9.612263188653356]
        cleanedlist = [[i, conc] for i, conc in enumerate(raw_M_list) if str(conc) != 'nan'] #e.g. [[3, 9.612263188653356]]
        molarity_list.append([element[1] for element in cleanedlist])  # 9.612263188653356
        temp_id.append([x.index[element[0]][1] for element in cleanedlist])
    molarity_list.extend(temp_id)
    sorted_dict = {x.index[0][0] : molarity_list}
    mycolumns = x.columns.tolist()
    mycolumns.extend(f'{colname}_inchikey' for colname in x.columns)
    sorted_df = pd.DataFrame.from_dict(sorted_dict,
                                       orient='index',
                                       columns=mycolumns)

    return(sorted_df)


def runuid_feat_merge(sumbytype_byinstance_molarity_df, inchi_key_indexed_features_df):
    """ Merge and rename function for runuid + report_feats dataframes

    Parameters
    ----------
    sumbytype_byinstance_molarity_df : pandas.DataFrame, contents matter!
        runUID 

    Notes
    ------
    Currently set to rename smiles, standardized smiles, and type information
    to _raw_ section of the dataframe.  

    """
    chemical_type_inchi = \
        sumbytype_byinstance_molarity_df.filter(regex='_inchikey')
    chemical_type_inchi.fillna('null', inplace=True)
    for type_inchi_col in chemical_type_inchi.columns:
        chemical_type = type_inchi_col.split('_')[2].strip() # _raw_inorganic_0_inchikey to inorganic
        feature_prefix = type_inchi_col.rsplit('_', 1)[0].strip().split('_',2)[2] # _raw_inorganic_0_inchikey to inorganic_0
        bulk_features = inchi_key_indexed_features_df.copy()
        bulk_features = bulk_features[bulk_features['types'].str.contains(pat=f'(?:^|\W){chemical_type}(?:$|\W)', regex=True)]
        #Drop anycolumns which are not full (likely due to specifying multiple types)
        bulk_features.dropna(axis=1, how='any', inplace=True)

        #Rename columns to fit with ESCALATE naming scheme (Brute force, not elegant)
        column_rename = {}
        drop_list = []
        known_drops = ['smiles_standardized', 'smiles', 'types']
        for column in bulk_features.columns:
            if any(column == known_drop for known_drop in known_drops):
                column_rename[column] = column
                drop_list.append(column)
            elif 'XXPASSTHROUGHXX' in column:
                column_rename[column] = column.split('_', 1)[1] # XXPASSTHROUGHXX_<name> to <name>
                drop_list.append(column)
            elif 'feat' in column:
                newcolumnname = column.split('_', 2)[2] # _feat_asavdwp to asavdwp
                column_rename[column] = newcolumnname

        raw_features_df = bulk_features.loc[:,drop_list]
        raw_features_df.rename(columns=column_rename, inplace=True)
        raw_features_df = raw_features_df.add_prefix(f'_raw_{feature_prefix}_')

        bulk_features.drop(drop_list, inplace=True, axis=1)
        bulk_features.rename(columns=column_rename, inplace=True)

        bulk_features = bulk_features.add_prefix(f'_feat_{feature_prefix}_')
        #TODO: rename based on the pass through filter '_feat_XXPASSTHROUGHXX_' to '_raw_<featname>'

        bulk_features = bulk_features.join(raw_features_df, on='inchikeys')

        chemical_type_inchi = chemical_type_inchi.join(bulk_features, on=type_inchi_col, rsuffix='DROPME_AFTER_MERGE')
    chemical_type_inchi.dropna(axis=1, how='all', inplace=True)
    final_drop_list = chemical_type_inchi.filter(regex='DROPME_AFTER_MERGE').columns
    chemical_type_inchi.drop(final_drop_list, inplace=True, axis=1)
    chemical_type_inchi.fillna(0, inplace=True)
    return chemical_type_inchi