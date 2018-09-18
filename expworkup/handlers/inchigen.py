import pandas as pd

def GrabInchi(rxn_mmol_df, labels_df):
## ignore GBL ##
    #Wierd pythoness required me to break this out into a list...
    label_list=[]
    dump=[]
    for item in labels_df:
        label_list.append(str(item))
    label_list_df=pd.DataFrame(label_list, columns=['RunID_vial'])
    ## and literally rebuild the dataframe.  Like.. email me if you figure out why this was needed
    inchi_list=[]
    index=0
    header_list=[]
    for header in list(rxn_mmol_df):
        header_list.append(header)
    for row_label, row in rxn_mmol_df.iterrows():
        dump.append(row_label)
        row_index=0
        for entry in row:
            if entry==0:
                pass
            else:
                header=header_list[row_index]
                InChIKey=header[10:-6]
                if InChIKey == "BDAGIHXWWSANSR-UHFFFAOYSA-N":
                    pass
                elif InChIKey =='YEJRWHAVMIAJKC-UHFFFAOYSA-N': 
                    pass
                elif InChIKey == 'RQQRAHKHDFPBMC-UHFFFAOYSA-L':
                    pass
                elif InChIKey == 'null':
                    pass
                else:
                    inchi_list.append(InChIKey)
                    index+=1
            row_index+=1
    keylist_df=pd.DataFrame(inchi_list, columns=['_rxn_organic-inchikey'])
    out_df=pd.concat([keylist_df, label_list_df], axis=1)
    out_df.set_index('RunID_vial', inplace=True)
    return(out_df)