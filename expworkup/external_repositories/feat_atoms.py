import pandas as pd 
from pathlib import Path, PurePath
from collections import Counter
import os

from rdkit import Chem

# TODO validate available atomic properties prior to allowing user to "grab"

def get_atomic_property(atom_count_df, target_data_name, short_name):
    """
    """
    lookup_data_path = Path('./expworkup/external_repositories/lookup-data/')
    all_files = ['Abbreviation.table', f'{target_data_name}.table']
    df = pd.concat((pd.read_csv(os.path.join(lookup_data_path, f)) for f in all_files), ignore_index=True, axis=1)
    df.columns = [f.split('.')[0] for f in all_files]
    df.set_index('Abbreviation', inplace=True)
    pd.to_numeric(df[target_data_name], errors='coerce')
    

    for atom_name in atom_count_df.index:
        atom_stoichiometry_df = f'XXPASSTHROUGHXX_{atom_name}_count'
        target_atom_name = f'XXPASSTHROUGHXX_{short_name.lower()}_{atom_name}'
        atom_count_df[atom_stoichiometry_df] =  atom_count_df.loc[atom_name, 'stoichiometry']
        atom_count_df[target_atom_name] = df.loc[atom_name, target_data_name]
    return atom_count_df


def get_atoms_df(m):
    """Get datafrelement symbol

    Returns
    ------

    """
    atoms_list = []
    for atom in m.GetAtoms():
        atoms_list.append(atom.GetSymbol()) # Create list of ['C', 'N', 'C', ...]
    atom_dict = Counter(atoms_list) # creates dictionary {'C': #instances ... , 'N':}
    atom_df = pd.DataFrame.from_dict(atom_dict, orient='index', columns=['stoichiometry'])
    return(atom_df)

def grab_atomic_property(identity_df, command_dict, ignore_atoms=None):
    """
    gets molecular formula, generates a column for each atom with the designated property
    """
    identity_df.set_index('smiles', inplace=True)
    out_df = pd.DataFrame()
    for smiles in identity_df.index:
        mymol = Chem.MolFromSmiles(smiles)
        target_data_name = command_dict['command'][0]
        atom_count_df = get_atoms_df(mymol)
        short_name = command_dict['column_names'][0].split('_', 2)[2] #_feat_<shortname> to <shortname> from feat_command
        atom_property_df = get_atomic_property(atom_count_df, target_data_name, short_name) #get first shortname only
        atom_property_df['inchikeys'] = identity_df.loc[smiles, 'inchikeys']
        atom_property_df.drop(['stoichiometry'], axis=1,  inplace=True)
        atom_property_df.drop_duplicates(inplace=True)
        atom_property_df.set_index('inchikeys', inplace=True)
        out_df = out_df.append(atom_property_df)
    out_df.fillna(0, inplace=True)
    return(out_df)
    
