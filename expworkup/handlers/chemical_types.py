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
    return(pd.Series((types_list, smiles)))