import pandas as pd
import numpy as np
import logging
import json

from itertools import permutations
from simpleeval import simple_eval
from collections import OrderedDict

from utils.globals import compound_ingredient_chemical_return
from utils.file_handling import write_debug_file
from utils.calc_command import CALC_COMMAND_DICT

modlog = logging.getLogger(f'mainlog.{__name__}')
warnlog = logging.getLogger(f'warning.{__name__}')

def get_mmol_df(reagent_volumes_df, 
                object_df, 
                chemical_count, 
                conc_model='default_conc'):
    """ returns the calculated mmol of each chemical 
     options defined by expworkup'solud_conc', 'solv_conc', default_conc]):
    """
    mmol_df = pd.DataFrame()

    modlog.info("mmol calculations and df creation")
    for reagent in reagent_volumes_df.columns:
        new_column_list = []
        convert_name = (reagent.rsplit('_', 1)[0].split('_', 1)[1]) #_raw_reagent_0_volume to raw_reagent_0
        new_column_list.extend([f'_{convert_name}_chemicals_{i}_mmol' for i in range(chemical_count)])

        #for each reagent, gather the concentrations of the associated chemicals in each reagent
        conc_df_temp = \
            object_df.loc[:, convert_name].apply(lambda x: 
                                           compound_ingredient_chemical_return(x, 
                                                                               chemical_count, 
                                                                               conc_model))

        # (M / L  * volume (uL) * (1L / 1000mL) * (1mL / 1000uL) * (1000mmol / 1mol) = mmol 
        mmol_df_temp = \
            conc_df_temp.loc[:,].multiply(reagent_volumes_df[reagent], 
                                          axis='index') / 1000

        mmol_df_temp.columns = new_column_list
        #possible TODO: add validation using the inchikey reads from the report_df
        mmol_df = mmol_df.join(mmol_df_temp, how='outer')
    modlog.info("Completed: 'mmol calculations and df creation'")
    return mmol_df

def all_ratios(df, fill_value, prefix):
    """will calculate the ratio for each pairing of columns in df
        AB and BA for all

    Parameters
    ----------
    df : pd.DataFrame index as name:(runUID) and only columns to permute

    fill_value : value to fill infinity, fails and blank with 
        often read from the type_command.csv interface

    Returns
    -------
    df2 : pd.DataFrame  index as name:(runUID) and permuted columns

    NOTE
    ---
    For ML, where linear functions matter a lot, the direction of the ratio
    will impact the ability of the model to use a given metric, AB and BA are
    likely both necessary.
    """
    # treating nulls as 0 here is fine, we know that they have 0
    df = df.replace('null', 0)
    cc = list(permutations(df.columns,2))
    df2 = pd.concat([df[c[1]].div(df[c[0]]) for c in cc], axis=1, keys=cc)
    df2.columns = df2.columns.map('_'.join)

    # Cleanup column headers
    df2 = df2.add_prefix(prefix)

    # Cleanup infinities and blanks
    df2 = df2.replace([np.inf, -np.inf, np.nan], fill_value)
    return(df2)

def df_simple_eval(command, variables, x, command_function=None, my_filter=False):
    """ Performs safe evals on dataframe

    Uses specified command with variable mapping onto x to generate numerical 
    (pd.DataFrame.dtypes number) values.

    Parameters
    ----------
    command :  arthematic expression structure e.g. "a + b"

    variables : dict, mapping for variables in command onto available columns

        columns are limited to pd.DataFrame.dtype = 'number'
        e.g. {"a": "_rxn_molarity_acid"...}

    x : row of dataframe which contains numerical data from columns used in variable

        (often specified as a lambda function from larger dataframe)

    Returns
    -------
    out_value : calculated value generated from applying 'command' to 
    the 'variables' in 'x'

    Notes
    -----
    See: https://github.com/danthedeckie/simpleeval for more information
        on supported evaluations
    """
    df_referenced_dict = {}
    if my_filter:
        for variable_name in variables.keys():
            df_referenced_dict[variable_name] = x.filter(regex=variables[variable_name])
    else:           
        for variable_name in variables.keys():
            df_referenced_dict[variable_name] = x[variables[variable_name]]
    out_value = simple_eval(command, names=df_referenced_dict, functions=command_function)
    return out_value

def evaluation_pipeline(all_targets, debug_bool):
    """ Handles offloading of safeevals specified calc_command.json calculations

    Parameters
    ----------
    all_targets : pd.DataFrame, indexed on 'name' (runUID)
        columns should only be of pd.dtype 'number'
    
    debug_bool : CLI argument, True=Enable debugging
        if toggled on, code will export CSV files of each calc dataframe
    
    Returns
    -------
    calc_df : pd.DataFrame, indexed on 'name' (runUID)
        columns are those generated and named by calc_command.json
    """ 
    calc_df = pd.DataFrame()
    calc_df['name'] = all_targets.index
    calc_df.set_index('name', inplace=True)
    eval_dict = CALC_COMMAND_DICT

    for entry_name in eval_dict.keys():
        header_name = entry_name

        command = eval_dict[entry_name].get('command', None)
        variables = eval_dict[entry_name].get('variable_names', None)
        column_infer = eval_dict[entry_name].get('use_regex', False)

        # We don't want the code to bomb out due to 
        # the calc_command.json not being properly constructed
        if command is None or variables is None:
            modlog.warn(f'For {entry_name}, command or variables were not correctly specified! Please check calc_command.json')
            warnlog.warn(f'For {entry_name}, command or variables were not correctly specified! Please check calc_command.json')
        else:
            # We don't want the code to bomb out due to 
            # all_targets not containing the specified headers, 
            run_function = True
            for x in variables.values():
                if column_infer:
                    run_function = True
                else:
                    if isinstance(x, str):
                        if not set(variables.values()).issubset(all_targets.columns):
                            modlog.warn(f"For {entry_name}, columns specified were not found! Please correct!")
                            warnlog.warn(f"For {entry_name}, columns specified were not found! Please correct!")
                            run_function = False
                    # Handle nested lists
                    elif isinstance(x, list):
                        if not set(x).issubset(all_targets.columns):
                            modlog.warn(f"For {entry_name}, columns specified were not found! Please correct!")
                            warnlog.warn(f"For {entry_name}, columns specified were not found! Please correct!")
                            run_function = False
            if run_function:
                fill_value = eval_dict[entry_name].get('fill_value', 'null')
                description = eval_dict[entry_name].get('description', 'null')
                specified_command = eval_dict[entry_name].get('functions', None)
                if fill_value == 'null':
                    modlog.info(f'For {entry_name}, "fill_value" was set to a default of "null"')
                if description == 'null':
                    modlog.info(f'For {entry_name}, "description" was set to a default of "null"')

                try:
                    value_column = all_targets.apply(lambda x: df_simple_eval(command, variables, x, command_function=specified_command, my_filter=column_infer), axis=1)
                except SyntaxError:
                    modlog.warn(f'For "{entry_name}", simpleeval failed to resolve the specified command, please check specification, or debug code!')        
                    warnlog.warn(f'For "{entry_name}", simpleeval failed to resolve the specified command, please check specification, or debug code!')        
                    value_column = pd.Series([np.nan]*len(all_targets), index=all_targets.index)

                value_column.rename(header_name, inplace=True)
                value_column.fillna(value=fill_value, inplace=True)

                if debug_bool:
                    debug_df = value_column.copy().to_frame()
                    for key, value in variables.items():
                        try:
                            debug_df[key] = all_targets[[value]]
                        except KeyError:
                            warnstring = f'Nested function used in calcs, will not export all columns'
                            modlog.warn(warnstring)
                            debug_df['warn'] = warnstring
                            pass
                    debug_df['variables'] = str(variables)
                    debug_df['command'] = command
                    debug_df['description'] = description
                    calc_file = f'{entry_name.upper()}.csv'
                    write_debug_file(debug_df, calc_file)

                calc_df = calc_df.join(value_column)
                #We also want new calcs accessible if possible
                all_targets = all_targets.join(value_column)

    return calc_df
    
