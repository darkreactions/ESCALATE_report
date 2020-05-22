import pandas as pd 
from pathlib import Path

def quantity_column_split(string):
    """ Quantity[100.4, "' Centimeters"^3/"Moles'"] to [100.4, " Centimeters"^3/"Moles"]
    """
    values = string.split("[", 1)[1][:-1].split(",")
    return(values)
    
def load_hansen_triples():
    """ read in a parse the hansen json

    See README.md file for more information 
    """
    hansen_csv = Path(__file__).parent / "JS_HansenSolubilityParameters.csv"
    hansen_df = pd.read_csv(hansen_csv, index_col='InChIKey')
    hansen_df.columns = hansen_df.columns.str.replace("δ", 'delta')

    # We have to break up each of the amounts into quantity and units columns
    amounts_list = ['Volume', 'deltad', 'deltap', 'deltah', 'deltat']
    curated_hansen_df = pd.DataFrame()
    for column in amounts_list:
        temp_df = hansen_df[column].apply(lambda x: pd.Series(quantity_column_split(x)))
        temp_df.columns = [column + '_amount', column + '_units']
        curated_hansen_df = pd.concat([curated_hansen_df, temp_df], axis=1)
    
    return(curated_hansen_df)

def get_hansen_triples(inchi_list, command_dict):
    """Create dataframe from hansen triples using the specified inchilist 

    Parameters
    ----------
    inchi_list : list of the inchi_keys to user in df generation

    command_dict : structure shown below
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
    Returns
    -------
    feature_df : pd.DataFrame of Hansen parameters 
        no index, but first column is the inchikeys

    """
    feature_df = pd.DataFrame(inchi_list, columns=['InChIKey'])
    hansen_df = load_hansen_triples()
    feature_df = feature_df.join(hansen_df, on='InChIKey')
    #can only have one prefix, take the column name from command dict
    feature_df.set_index('InChIKey', inplace=True)
    #Set header names for return probably don't want to drop units... but maybe
    #units_columns = feature_df.filter(like='units', axis=1)
    values_columns = feature_df.filter(like='amount', axis=1)
    values_columns = values_columns.add_prefix(f'{command_dict["column_names"][0]}_') 

    feature_df = values_columns.astype('float')
    #units_columns = units_columns.add_prefix(f'_raw_raw{command_dict["column_names"][0]}_') 
    #feature_df = values_columns.join(units_columns)
    feature_df.drop_duplicates(keep='first', inplace=True)
    #cleanup for export
    feature_df = feature_df.reset_index().rename(columns={'InChIKey':'inchikeys'})
    return(feature_df)

