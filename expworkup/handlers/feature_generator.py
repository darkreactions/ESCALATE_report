import pandas as pd
import json
import logging

from chemdescriptor.generator.chemaxon import ChemAxonDescriptorGenerator as cag
from chemdescriptor.generator.rdkit import RDKitDescriptorGenerator as rdg

from utils.globals import get_offline_folder, get_target_folder, get_log_folder
from utils.file_handling import get_command_dict
from expworkup.devconfig import CALC_POSSIBLE, STANDARDIZE_POSSIBLE, CXCALC_PATH 
from expworkup.external_repositories.feat_hansen import get_hansen_triples

modlog = logging.getLogger(f'mainlog.{__name__}')
warnlog = logging.getLogger(f'warning.{__name__}')

class OneTypeFeatures():
    """
    Attributes
    ----------

    NOTE: Currently supports: cxcalc, cxcalc_std, RDKit, escalate
    # TODO: insert validation of the type_command df
    # type_command df should be treated as code!
    """
    def __init__(self,
                 one_type,
                 onetype_feature_identity_dict):
        self.one_type = one_type
        self.smiles_list = onetype_feature_identity_dict['smiles'].values.tolist()
        self.inchi_list = onetype_feature_identity_dict['inchikeys'].values.tolist()
        self.cxcalc_command_dict = get_command_dict(self.one_type,
                                                    'cxcalc')
        self.cxcalcstd_command_dict = get_command_dict(self.one_type,
                                                       'cxcalc_std')
        self.rdkit_command_dict = get_command_dict(one_type,
                                                   'RDKit')
        self.escalate_command_dict = get_command_dict(one_type,
                                                      'EscalateFeats')
        self.featured_df = self.generate_onetype_features(onetype_feature_identity_dict,
                                                          self.cxcalc_command_dict,
                                                          self.cxcalcstd_command_dict,
                                                          self.rdkit_command_dict,
                                                          self.escalate_command_dict)

    def cxcalc_handler(self,
                       cxcalc_command_dict,
                       smiles_list,
                       one_type,
                       standardize_bool):
        """ Wrap chemdescriptor function for interaction with ESCALATE and type_command.csv format
            Includes more robust error reporting for header renaming (short_name from type_command.csv)
            Allows flexible specification of standardization functions actor_org: cxcalc_std vs cxcalc

        Parameters
        ----------
        cxcalc_command_dict : dict, see structure in 'get_command_dict' func

        smiles_list : list of smiles string to be used to generate features

        one_type : label of the chemical type of features
            can be any string, used to label error messages

        standardize_bool : bool, standardize smiles in smiles_list?
            standardize flag to be passed to chemdescriptor chemaxon function

        Returns
        -------
        type_features_df : pandas.DataFrame, smiles with generated features 
        """
        try:
            calc_features = cag(smiles_list,
                                whitelist={},
                                command_dict=cxcalc_command_dict,
                                logfile=f'{get_log_folder()}/CXCALC_LOG.txt',
                                standardize=standardize_bool)
            type_features_df = \
                calc_features.generate(f'./{get_offline_folder()}/CXCALC_{one_type}_FEATURES.csv',
                                       dataframe=True,
                                       rename_columns=True)

            # Alert the user if the autorename didn't work.  
            # This is hard to automate, ChemAxon doesn't document what it returns
            feature_rename_list = []
            for feature in cxcalc_command_dict['descriptors'].keys():
                feature_rename_list.extend(cxcalc_command_dict['descriptors'][feature]['column_names'])

            # -1 to remove the smiles identity column from the count
            # if these are the same length, the chemaxon rename will go through
            # if not, the chemaxon default columns will be returned
            if (type_features_df.shape[1]-1) != len(feature_rename_list):
                # This error actually means that the headers in the return will be dependent upon chemaxon defaults
                # USER may not know what the headers correspond to.  ChemAxon docs are crap and there is no way to 
                # gather that information from an API call
                modlog.warn(f'Rename of ChemAxon {one_type} descriptors was unsuccessful. Please verify user specified column names in type_command.csv!')
                warnlog.warn(f'Rename of ChemAxon {one_type} descriptors was unsuccessful. Please verify user specified names in type_command.csv!')

        except UnboundLocalError:
            # Option could be to pass on despite fail, this is problematic as it implies the USER is getting 
            # the data they wanted.
            modlog.error(f'Critical Error: cxcalc functions incorrectly specified. Please validate type_command.csv!')
            warnlog.error(f'Critical Error: cxcalc functions incorrectly specified. Please validate type_command.csv!')
            import sys
            sys.exit()

        return type_features_df

    def rdkit_handler(self,
                      rdkit_command_dict, 
                      one_type,
                      smiles_list):
        """ Wrap chemdescriptor rdkit_handler function for interaction with ESCALATE and type_command.csv 
        Includes more robust error reporting for header renaming (short_name from type_command.csv)

        Parameters
        ----------
        rdkit_command_dict : dict, see structure in 'get_command_dict' func

        smiles_list : list of smiles string to be used to generate features

        one_type : label of the chemical type of features
            can be any string, used to label error message

        Returns
        -------
        type_features_df : pandas.DataFrame, smiles with generated features
        """
        try:
            rdkit_features = rdg(smiles_list,
                                 whitelist=rdkit_command_dict,
                                 logfile=f'{get_log_folder()}/RDKIT_LOG.txt')
            type_features_df = rdkit_features.generate(f'./{get_offline_folder()}/RDKIT_{one_type}_FEATURES.csv',
                                                       dataframe=True)
        except UnboundLocalError:
            modlog.error(f'Critical Error: cxcalc functions incorrectly specified. Please validate type_command.csv!')
            warnlog.error(f'Critical Error: cxcalc functions incorrectly specified. Please validate type_command.csv!')
            import sys
            sys.exit()

        feature_rename_list = []
        for feature in rdkit_command_dict['descriptors'].keys():
            feature_rename_list.extend(rdkit_command_dict['descriptors'][feature]['column_names'])

        if (type_features_df.shape[1]-1) > len(feature_rename_list):
            modlog.warn(f'RDKit features for {one_type} were incorrectly specified. Outputting ALL available options for the smiles. Please verify type_command.csv to reduce output')
            warnlog.warn(f'RDKit features for {one_type} were incorrectly specified. Outputting ALL available options for the smiles. Please verify type_command.csv to reduce output')

        # Drop duplicate columns on join
        return type_features_df
    
    def escalatefeat_handler(self,
                             escalate_command_dict,
                             one_type,
                             inchi_list):
        """One by one handling of local features

        The pipelines in this function could effectively be any table of data
        deliniated by inchikey.  Once enough examples are present in the
        'external_repositories' this function can be generalized.  Only one for now

        Parameters
        ----------
        
        escalate_command_dict : dict, see structure in 'get_command_dict' func

        one_type : label of the chemical type of features
            can be any string, used to label error message

        smiles_list : list of inchi strings to be used to generate features
        
        Returns
        -------
        type_features_df : pandas.DataFrame, smiles with generated features       
        """
        escalate_descriptors = escalate_command_dict['descriptors']
        type_features_df = get_hansen_triples(inchi_list,
                                              escalate_descriptors['hansentriple'])

        return type_features_df


    def generate_onetype_features(self,
                                  outdf,
                                  cxcalc_command_dict,
                                  cxcalcstd_command_dict,
                                  rdkit_command_dict,
                                  escalate_command_dict):
        """Handles the offloading of feature calculations 

        Parameters
        ----------
        outdf : pandas.DataFrame, aka onetype_feature_identity_dict
            input dataframe to OneTypeFeatures containing inchikey, smiles information

        cxcalc_command_dict : dict, see structure in 'get_command_dict' func

        cxcalcstd_command_dict : dict, see structure in 'get_command_dict' func

        rdkit_command_dict : dict, see structure in 'get_command_dict' func

        escalate_command_dict : dict, see structure in 'get_command_dict' func

        Returns
        -------
        outdf : pandas.DataFrame, all features for chemicals in one_type
            indexed on 'inchikeys', columns CANNOT be dtype=pandas.DataFrame
        """
        if cxcalc_command_dict is not None:
            if CALC_POSSIBLE:
                type_features_df = self.cxcalc_handler(cxcalc_command_dict,
                                                       self.smiles_list,
                                                       self.one_type,
                                                       False)
                outdf = pd.concat([outdf,
                                   type_features_df], axis=1)
            else:
                warnlog.warn(f'Devconfig cxcalc path is {CXCALC_PATH}, no such file exists! Skipping ChemAxon Functions')
                modlog.warn(f'Devconfig cxcalc path is {CXCALC_PATH}, no such file exists! Skipping ChemAxon Functions')

        if cxcalcstd_command_dict is not None:
            if CALC_POSSIBLE and STANDARDIZE_POSSIBLE:
                type_features_df = self.cxcalc_handler(cxcalcstd_command_dict,
                                                       self.smiles_list,
                                                       self.one_type,
                                                       True)
                type_features_df.rename(columns={"Compound": "smiles_standardized"}, inplace=True)
                outdf = pd.concat([outdf,
                                   type_features_df], axis=1)
            else:
                warnlog.warn(f'Devconfig standardizer path is {CXCALC_PATH}, no such file exists! Skipping Standardizer functions')
                modlog.warn(f'Devconfig standardizer path is {CXCALC_PATH}, no such file exists! Skipping Standardizer functions')

        if rdkit_command_dict is not None:
            type_features_df = self.rdkit_handler(rdkit_command_dict,
                                                  self.one_type,
                                                  self.smiles_list)
            type_features_df = \
                type_features_df.loc[:,~type_features_df.columns.duplicated()]
            outdf = pd.concat([outdf, 
                               type_features_df], axis=1)
        
        if escalate_command_dict is not None:
            type_features_df = self.escalatefeat_handler(escalate_command_dict,
                                                        self.one_type,
                                                        self.inchi_list)
            outdf = pd.concat([outdf, 
                               type_features_df], axis=1)

        try:
            outdf = \
                outdf.loc[:,~outdf.columns.duplicated(keep='last')]
        except KeyError:
            modlog.warn(f'No features defined for {self.one_type}')
            pass
        outdf.set_index('inchikeys', inplace=True)
        return outdf