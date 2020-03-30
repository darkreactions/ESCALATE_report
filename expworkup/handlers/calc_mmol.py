import pandas as pd
import logging

modlog = logging.getLogger(__name__)

def combine(portioned_df):
    combined_df=pd.DataFrame()
    reagentlist=[]
    mmol_df_dict={}
    for header in list(portioned_df):
        # prevents trying to sum a single item and instead just adds the series back to a dataframe for later use
        if len(list(portioned_df[header].shape)) == 1:
            header_df_name=header+"_final"
            temp_df=pd.DataFrame(portioned_df[header])
            temp_df.columns=[header_df_name]
            mmol_df_dict[header]=temp_df
        else: 
            if header in reagentlist:
                pass
        #Takes all dataframe columns with name "header" and sums the values together returning the summed list to a header referenced dict
        #Probably a better way to do this, but it works for all dataframes in the test set
            else:
                mmol_value=[]
                for index, experiment in portioned_df[header].iterrows():
                    mmol_list=[]
                    for item in experiment:
                        mmol_list.append(item)
                    mmol_value.append((sum(mmol_list)))
                header_df_name=header+"_final"
                mmol_df_dict[header]=pd.DataFrame(mmol_value, columns=[header_df_name])
                reagentlist.append(header)
    #outputs all of the dataframes created during the mmol summing process and returns with new header for later
    for (k,v) in mmol_df_dict.items():
        combined_df=pd.concat([combined_df, v], axis=1)
    return(combined_df)

def calc_mmol_v1(vol, index, reagent_name, JsonParsed_df):
    """
    calculates the mmol of the reagent and returns to a list (v1 using new concentration model)

    order is important as the indexes are not maintained through this step)
    The calculation is done indpendently to better handle different chemicals
    (and thereby be more flexible moving forward) using the index



    :param vol:
    :param index:
    :param reagent_name:
    :param JsonParsed_df:
    :return:
    """
    #    print(vol, index, reagent_name)
    mmol_cell={}
    for header in list(JsonParsed_df):
        if ('_v1-conc_' in header) and (reagent_name in header):
            mmol_name=('_raw_v1-mmol_' + header[23:])
            #            print(((vol*JsonParsed_df.loc[index, header]/1000)), header)
            mmol_cell[mmol_name]=((vol*JsonParsed_df.loc[index, header]/1000))
    #Returns a dictionary with the key set to the _raw_mmol + inchi string taken from the parsed mmol name above.
    return(mmol_cell)

def calc_mmol(vol, index, reagent_name, JsonParsed_df):
    """
    calculates the mmol of the reagent and returns to a list

    order is important as the indexes are not maintained through this step)
    The calculation is done indpendently to better handle different chemicals
    (and thereby be more flexible moving forward) using the index



    :param vol:
    :param index:
    :param reagent_name:
    :param JsonParsed_df:
    :return:
    """
#    print(vol, index, reagent_name)
    mmol_cell={}
    for header in list(JsonParsed_df):
        if ('_conc_' in header) and (reagent_name in header):
            mmol_name=('_raw_mmol_' + header[20:])
#            print(((vol*JsonParsed_df.loc[index, header]/1000)), header)
            mmol_cell[mmol_name]=((vol*JsonParsed_df.loc[index, header]/1000))
    #Returns a dictionary with the key set to the _raw_mmol + inchi string taken from the parsed mmol name above. 
    return(mmol_cell)

def volcheck(vol_series, reagent_name, JsonParsed_df,runID_df):
    """
    Converts volumes of reagents into mmol of chemicals

    :param vol_series: series of volumes from a reagent in a given experiment
    :param reagent_name: reagent under consideration
    :param JsonParsed_df: full json file from the parsed json (likely overkill and can be reduced)
    :param runID_df: runID of the given set of experiments
    :return: dataframe of calculated mmol from v0 concentration and v1 concentration
    """
    index=0
    mmol_index_list=[]
    mmol_index_list_v1=[]
    for volume in vol_series:
            runID=runID_df.loc[index,'RunID_vial']
            mmol_index_list.append(calc_mmol(volume, index, reagent_name, JsonParsed_df))
            mmol_index_list_v1.append(calc_mmol_v1(volume, index, reagent_name, JsonParsed_df))
            index+=1
    mmol_df_out=pd.DataFrame(mmol_index_list)
    mmol_index_list_v1 = pd.DataFrame(mmol_index_list_v1)
    if mmol_df_out.size == 0:
        pass
    else:
        mmol_df_out = pd.concat([mmol_df_out, mmol_index_list_v1], axis=1)
        return(mmol_df_out)

#splits of the reagents to analyze the volumes of the important reagents for the current workflows (wkflow1.1 is 2,3,4)
def mmol_breakoff(JsonParsed_df, runID_df):

    out_df = pd.DataFrame()
    reagent_mmol_df=pd.DataFrame()

    for columnname in list(JsonParsed_df):
        if ("_raw_reagent_" in columnname) and ("_volume" in columnname):
            if columnname == "_raw_reagent_0_volume":
                pass
            else:
                reagent_name=columnname[:-6]
                reagent_mmol_df = volcheck(JsonParsed_df[columnname], reagent_name, JsonParsed_df, runID_df)

            out_df=pd.concat([out_df, reagent_mmol_df], axis=1, sort=False)

    out_combined_df=combine(out_df)  
    return(out_combined_df)