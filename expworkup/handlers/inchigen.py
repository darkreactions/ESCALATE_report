import pandas as pd
import sys
import logging

modlog = logging.getLogger(f'mainlog.{__name__}')
warnlog = logging.getLogger(f'warning.{__name__}')

def GrabOrganicInchi(inchi_df, molaritydf):
    """ Converts dataframe of inchi keys to a single column of 'organic' Inchis

    Takes a dataframe of runID indexed InChIkeys and uses hard coded parameters
    to determine the organic component in each experiment.  

    *Current implementation assumes that the chemistry is limited to
    perovskite ontologies.  The solvents, inorganics, and acids are 
    all hard coded.  The remaining inchikeys are assumed to be associated
    with the organics.

    *Cannot handle multiple organics in a single experiment, this will require
    generalizing the subsequent feature merger code as well    

    """
#    molaritydf molaritydf_in.filter(regex='')
    #TODO: generalize, import the correct df (miinimum to avoid errors)
    inchi_dict = {}
    for row_label, row in inchi_df.iterrows():
        lastrows=None
        for InChIKey in row:
            # pass if formic acid (hard coded inchi key)
            if InChIKey == "BDAGIHXWWSANSR-UHFFFAOYSA-N":
                pass
            # pass if inchi is solvent
            elif InChIKey == 'YEJRWHAVMIAJKC-UHFFFAOYSA-N' or \
                    InChIKey == 'ZMXDDKWLCZADIW-UHFFFAOYSA-N' or\
                    InChIKey == 'IAZDPXIOMUYVGZ-UHFFFAOYSA-N' or\
                    InChIKey == 'YMWUJEATGCHHMB-UHFFFAOYSA-N' or\
                    InChIKey == 'MVPPADPHJFYWMZ-UHFFFAOYSA-N' or\
                    InChIKey == 'UserDefinedSolvent':
                pass
            # pass if lead iodide
            elif InChIKey == 'RQQRAHKHDFPBMC-UHFFFAOYSA-L' or \
                    InChIKey == 'ZASWJUOMEGBQCQ-UHFFFAOYSA-L':
                pass
            elif InChIKey == 'null' or InChIKey == 0: pass
            else:
                molarityorganicdf = molaritydf.filter(regex=InChIKey)
#                totalmolarity_organic = (sum(molarityorganicdf.loc[row_label].values.tolist()))
                totalmolarity_organic = (molarityorganicdf.loc[row_label].sum()) # something fishy happens where these drop to pandas series (often a bug)
                if isinstance(totalmolarity_organic, pd.core.series.Series):
                    print('total molarity organic', totalmolarity_organic)
                    print('the df', molarityorganicdf)
                    print('row_label', row_label)#(possibly useful for debugging))
                    modlog.error(f'{row_label} or {lastrows} are likely corrupt, if this is unexpected try deleting this JSON and starting again')
                    modlog.error(f"Rendered JSON files in selected folder are somehow corrupt. \
                         Please delete folder and start again if you are unable to diagnose the problem")
                # error handling to remove folder and recompile when a run fails
                    sys.exit()
                if totalmolarity_organic != 0:
                    orgInChIKey = InChIKey
                else:
                    pass
            lastrows=row_label
        inchi_dict[row_label] = orgInChIKey
    keylist_df = pd.DataFrame.from_dict(inchi_dict, orient='index')
    keylist_df.rename(columns={list(keylist_df)[0]: '_rxn_organic-inchikey'},
                      inplace=True)
    keylist_df.index.name = 'RunID_vial'
#    keylist_df.set_index('RunID_vial', inplace=True)
    return(keylist_df)


def GrabInchi(rxn_mmol_df, labels_df, inchi_df):
    """Depreciated:  Condenses ESCALATE dataframe, returns organicinchi

    This code is designed with the assumption that only a single organic 
    will be used in any given experiment.  Hard coded use of solvents, 
    inorganic, and acids in the current implementation.

    Returns a single column of organic concentrations as well as the 
    associated inchikey
    """
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
#            print(label_list[index], entry, index, row_index)
            if entry==0:
#                print(label_list[index], entry, header, row_index)
                pass
            else:
                header=header_list[row_index]
                InChIKey=header[10:-6]
                #pass if formic acid hard coded inchi key
                if InChIKey == "BDAGIHXWWSANSR-UHFFFAOYSA-N":
                    pass
                # pass if inchi is solvent
                elif InChIKey == 'YEJRWHAVMIAJKC-UHFFFAOYSA-N' or \
                        InChIKey == 'ZMXDDKWLCZADIW-UHFFFAOYSA-N' or \
                        InChIKey == 'IAZDPXIOMUYVGZ-UHFFFAOYSA-N' or \
                        InChIKey == 'YMWUJEATGCHHMB-UHFFFAOYSA-N' or \
                        InChIKey == 'MVPPADPHJFYWMZ-UHFFFAOYSA-N' or \
                        InChIKey == 'UserDefinedSolvent':
                    pass
                # pass if lead iodide
                elif InChIKey == 'RQQRAHKHDFPBMC-UHFFFAOYSA-L' or \
                        InChIKey == 'ZASWJUOMEGBQCQ-UHFFFAOYSA-L':
                    pass
                elif InChIKey == 'null':
                    pass
                else:
                    inchi_list.append(InChIKey)
                    index+=1
#                print(label_list[index], entry, header, row_index)
            row_index+=1
    keylist_df=pd.DataFrame(inchi_list, columns=['_rxn_organic-inchikey'])
    out_df=pd.concat([keylist_df, label_list_df], axis=1)
    out_df.set_index('RunID_vial', inplace=True)
    return(out_df)