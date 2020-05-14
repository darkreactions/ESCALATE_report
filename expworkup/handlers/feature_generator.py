import pandas as pd
import json
import logging

from chemdescriptor.generator.chemaxon import ChemAxonDescriptorGenerator as cag
from chemdescriptor.generator.rdkit import RDKitDescriptorGenerator as rdg

from utils.globals import get_offline_folder, get_target_folder, get_log_folder

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
        self.type_command_df = pd.read_csv('./type_command.csv')
        self.smiles_list = onetype_feature_identity_dict['smiles'].values.tolist()
        self.cxcalc_command_dict = self.get_command_dict(self.type_command_df,
                                                         self.one_type,
                                                         'cxcalc')
        self.cxcalcstd_command_dict = self.get_command_dict(self.type_command_df,
                                                            self.one_type,
                                                            'cxcalc_std')
        self.rdkit_command_dict = self.get_command_dict(self.type_command_df,
                                                        one_type,
                                                        'RDKit')
        self.featured_df = self.generate_onetype_features(onetype_feature_identity_dict,
                                                          self.cxcalc_command_dict,
                                                          self.cxcalcstd_command_dict,
                                                          self.rdkit_command_dict)


    def get_command_dict(self, command_type_df, one_type, application):
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
        commands_df = command_type_df[(command_type_df['input'] == one_type) &  \
                                      (command_type_df['actor_systemtool_name'] == application)]
        my_descriptor_dict = {}
        for command in commands_df.itertuples():
                column_name = f'_feat_{command.short_name}'
                my_descriptor_dict[command.short_name] = {}

                # 'space' (i.e, ' ') removal
                templist = command.calc_definition.split(' ')
                str_list = list(filter(None, templist))
                my_descriptor_dict[command.short_name]["command"] = str_list

                my_descriptor_dict[command.short_name]["column_names"] = [column_name]

        command_dict = {}
        command_dict['descriptors'] = my_descriptor_dict
        command_dict['ph_descriptors'] = {} # possibly useful, see chemdescriptor for more details
        if len(command_dict['descriptors'].keys()) == 0:
            return None
        else:
            return(command_dict)

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

        Returns
        -------

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
        """
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

    def generate_onetype_features(self,
                                  outdf,
                                  cxcalc_command_dict,
                                  cxcalcstd_command_dict,
                                  rdkit_command_dict):
        """
        """
        if cxcalc_command_dict is not None:
            type_features_df = self.cxcalc_handler(cxcalc_command_dict,
                                                   self.smiles_list,
                                                   self.one_type,
                                                   False)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
            outdf.rename(columns={"Compound": "calc_input_smiles"}, inplace=True)
>>>>>>> readability, streamline feats
=======
>>>>>>> resolved error with combine_first function, _raw_smiles export enabled
=======
            outdf.rename(columns={"Compound": "calc_input_smiles"}, inplace=True)
>>>>>>> readability, streamline feats
            outdf = pd.concat([outdf,
                               type_features_df], axis=1)

        if cxcalcstd_command_dict is not None:
            type_features_df = self.cxcalc_handler(cxcalcstd_command_dict,
                                                   self.smiles_list,
                                                   self.one_type,
                                                   True)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
            type_features_df.rename(columns={"Compound": "smiles_standardized"}, inplace=True)
=======
            outdf.rename(columns={"Compound": "smiles_standardized"}, inplace=True)
>>>>>>> readability, streamline feats
=======
            type_features_df.rename(columns={"Compound": "smiles_standardized"}, inplace=True)
>>>>>>> resolved error with combine_first function, _raw_smiles export enabled
=======
            outdf.rename(columns={"Compound": "smiles_standardized"}, inplace=True)
>>>>>>> readability, streamline feats
            outdf = pd.concat([outdf,
                               type_features_df], axis=1)

        if rdkit_command_dict is not None:
            type_features_df = self.rdkit_handler(rdkit_command_dict,
                                                  self.one_type,
                                                  self.smiles_list)
            type_features_df = \
                type_features_df.loc[:,~type_features_df.columns.duplicated()]
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