import pandas as pd

def cleaner(clean_df, raw_bool_cli):
    ''' Improves consistency of namespace and fills column blanks 
    
      * Removes spaces and symbols from the columns name space, 
      * Fills blanks entries based on the datatype ('null', 0, etc) 
    
    Parameters
    ----------
    clean_df : incoming dataframe from parsing the JSON file and renaming

    raw_bool_cli : cli argument, 
        if True includes extended dataframe including superfluous columns
        used in data handling

    Returns
    ---------
    squeaky_clean_df : the report_df after selected post-processing steps
        the bigly-est and cleanly-est dataset jsonparser can generate 
        (given the selected options, no refunds)


    Cleans the file in different ways for post-processing analysis
    '''

    null_list = []
    for column_type, column_name in zip(clean_df.dtypes, clean_df.columns):
        # we have to exclude all numerical values where '0' (zero) has meaning, e.g., temperature
        if column_type == 'object':
            null_list.append(column_name)
        if column_type != 'object' and '_rxn_' in column_name:
            null_list.append(column_name)
        if column_type != 'object' and '_units' in column_name:
            null_list.append(column_name)
        if '_out_' in column_name:
            null_list.append(column_name)
    clean_df[null_list] = clean_df[null_list].fillna(value='null')
    clean_df = clean_df.fillna(value=0)

    rxn_df = clean_df.filter(like='_rxn_')
    raw_df = clean_df.filter(like='_raw_')
    feat_df = clean_df.filter(like='_feat_') 
    out_df = clean_df.filter(like='_out_') 
    proto_df = clean_df.filter(like='_prototype_')

    if raw_bool_cli == 1:
        squeaky_clean_df = clean_df
    else:
        squeaky_clean_df = pd.concat([out_df, rxn_df,
                                      feat_df, raw_df,
                                      proto_df],
                                      axis=1)

    squeaky_clean_df.columns = map(str.lower, squeaky_clean_df.columns)
    return(squeaky_clean_df)