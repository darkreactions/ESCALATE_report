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
    """ Unique handler to gather unique main_type instances into individual columns
      molarity column and inchi column as list of lists

    TODO: add to schemas.py
    Parameters
    ----------


    Returns
    -------
    unique_chemicals_types_byinstance : pd. DataFrame
        include the [M]olarity of each unique chemical type in a given run
        along with the inchikey of the particular chemical. 
        Headers are consistent
        Ex.
                          acid_0  inorganic_0 inorganic_1 ... solvent_inchikey_0 
        name                                                              
        2017-10 ... _A1  2.923979 0.662557     NaN  YEJRWHAVMIAJKC-UHFFFAOYSA-N
        2017-10 ... _B1  4.779492 0.610422     NaN  YEJRWHAVMIAJKC-UHFFFAOYSA-N
        2017-10 ... _C1  4.779492 0.610422     NaN  YEJRWHAVMIAJKC-UHFFFAOYSA-N
        2017-10 ... _D1  4.194307 0.626864     NaN  YEJRWHAVMIAJKC-UHFFFAOYSA-N
        2017-10 ... _E1  3.576726 0.644217     NaN  YEJRWHAVMIAJKC-UHFFFAOYSA-N

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
        fixed_column = f'{column}_M'
        unique_chemicals_types_byinstance.rename(columns = {column:fixed_column}, inplace = True)
    return unique_chemicals_types_byinstance

def expand_columns(df, column):
    """ expands specified column with lists into multiple columns
    """
    df = pd.DataFrame(df[column].values.tolist(), df.index).add_prefix(f'{column}_')
    return df

def chemical_type_sorting(x):
    """ parse the lines of the molarity table to compress type values to a single column of lists
    """
    molarity_list = []
    temp_id = []
    for column in x.columns:
        raw_M_list = x[column].values.tolist()
        cleanedlist = [[i, conc] for i, conc in enumerate(raw_M_list) if str(conc) != 'nan']
#        cleanedlist = [x for x in raw_M_list if str(x) != 'nan']
        molarity_list.append([element[1] for element in cleanedlist])
        temp_id.append([x.index[element[0]][1] for element in cleanedlist])
    molarity_list.extend(temp_id)
    sorted_dict = {x.index[0][0] : molarity_list}
    mycolumns = x.columns.tolist()
    mycolumns.extend(f'{colname}_inchikey' for colname in x.columns)
    sorted_df = pd.DataFrame.from_dict(sorted_dict,
                                       orient='index',
                                       columns=mycolumns)
    return(sorted_df)