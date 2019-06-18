import pandas as pd

#hard coded around the notion that the last two entries are formic acid.  This will have to be changed!

def molarity_calc(raw_df, finalvol_entries):
    """
    calculate the molarity for each experiment

    :param raw_df:  the whole dataframe of all raw values
    :param finalvol_entries: final volume columns from the raw dataframe
    :return: chemical molarity dataframe for each experiment
    """
    ##Calculate Reagent volumes
    reagent_list=[]
    for header in raw_df.columns:
        if "volume" in header:
            reagent_list.append(header)
    total_list=reagent_list[:-finalvol_entries]
    df_total_list=raw_df[total_list]
    total_vol_df = df_total_list.sum(axis=1)
    df_final_vols=raw_df[reagent_list]
    df_final_vols.to_csv('FinalVols.csv')
    final_vol_df = df_final_vols.sum(axis=1)
    calculated_volumes_df=pd.concat([total_vol_df, final_vol_df],axis=1)
    calculated_volumes_df.columns=['_raw_total_volume', '_raw_final_volume']
    ## Calculate molarity (grab reagent mmols and then use the volumes caclualted above to detrmine the "nomial molarity")
    mmol_reagent_list=[]
    for header in raw_df.columns:
        if '_raw_mmol_' in header and 'final' in header:
            mmol_reagent_list.append(header)
    mmol_reagent_df = raw_df[mmol_reagent_list]    
    molarity_df = pd.DataFrame()
    for header in mmol_reagent_df:
        newheader='_raw_M_'+header[10:-6]+'_final'
        molarity_df[newheader] = mmol_reagent_df[header] / (calculated_volumes_df['_raw_final_volume']/1000)

    mmol_reagent_list_2=[]
    for header in raw_df.columns:
        if '_raw_v1-mmol_' in header and 'final' in header:
            mmol_reagent_list_2.append(header)
    mmol_reagent_df_2 = raw_df[mmol_reagent_list_2]
    molarity_df_2 = pd.DataFrame()
    for header in mmol_reagent_df_2:
        newheader2 = '_raw_v1-M_' + header[13:-6]+'_final'
        molarity_df_2[newheader2] = mmol_reagent_df_2[header] / (calculated_volumes_df['_raw_final_volume']/1000)
    molarity_df = pd.concat([molarity_df, molarity_df_2], axis=1)
    return(molarity_df)