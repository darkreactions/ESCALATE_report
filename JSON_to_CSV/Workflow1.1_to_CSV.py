import json
import pandas as pd
import argparse as ap
import os

parser = ap.ArgumentParser(description='Target Folder')
parser.add_argument('Filename', type=str, help='Please include target folder') 
args = parser.parse_args()
myjsonfol = args.Filename

#Flattens the list and returns the heirchical naming structure 0 ... 1 ... 2  ## See the example in the faltten_json_reg definition for more details
def flatten_json(y):
    out = {}
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[str('_raw_'+ name[:-1])] = str(x)
    flatten(y)
    return(pd.io.json.json_normalize(out))

#Flattens the list and returns the heirchical naming structure 0 ... 1 ... 2  (this function returns the reagent information in the proper namespace)
def flatten_json_reg(y):
    out = {}
    def flatten(x, name=''):
        #Flattens the dictionary type to a single column and entry
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        #Flattens list type to a single column and entry
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            ## Parses the namespace if the units are contained with the value from the JSON
            if ':' in str(x):
                x1,x2=x.split(":")
                out[str('_raw_reagent_' + name[:-1])] = x1 #Drops to the default namespace 
                out[str('_raw_reagent_' + name[:-1] + '_units')] = str(x2) # adds a new entry for the column with the units which is adjacent to the measurement and otherwise has an identical name
            else:
                out[str('_raw_reagent_' + name[:-1])] = str(x) #If the JSON value was a not a unit based value, the key value pair is returned as a column header and entry
    flatten(y) # Flattens the rest of the formating for pandas import
    return(pd.io.json.json_normalize(out)) # normalizes the data and reads into a single row data frame

#code for breaking out the list of lists and creating the relevant data frame (returns dataframe)
def dict_listoflists(list_lists):
    keys=[]
    values=[]
    for item in list_lists:
        key=str(item[0])
        key=str('_raw_'+key[:-1])
        keys.append(key)
        value=item[1]
        values.append(value)
    tray_df=pd.DataFrame(values, index=keys)
    return(tray_df.transpose())

#Will eventually create a dataframe from the robot handling information
def robo_handling():
    pass

def reagentparse(firstlevel, myjson):
    for reg_key,reg_value in firstlevel.items():
        if reg_key == 'reagent':
            reagent_df=flatten_json_reg(reg_value)
        if reg_key == 'run':
            run_df=flatten_json(reg_value)
        if reg_key == 'tray_environment':
            tray_df=dict_listoflists(reg_value)
##### Currently ommitting this line of data as the information here does not add detail to the data structure ### 
#        if reg_key == 'robot_reagent_handling':
#            robo_df=robo_handling(reg_value)
#                print(item)
########### See above note, still should parse later!!! after v1.1 ### 
        if reg_key == 'well_volumes':
            well_volumes_df=pd.DataFrame(reg_value, columns=['_raw_vialsite', '_raw_Reagent0', '_raw_Reagent1', '_raw_Reagent2', '_raw_Reagent3', '_raw_Reagent4', '_raw_Reagent5', '_raw_labwareID'])
        if reg_key == 'crys_file_data':
            crys_file_data_df=pd.DataFrame(reg_value, columns=['_raw_vialsite', '_raw_crystalscore'])
    #The following code aligns and normalizes the data frames
    experiment_df=well_volumes_df.merge(crys_file_data_df)
    wellcount=(len(experiment_df.index))-1
    fullrun_df=(run_df.append([run_df]*wellcount,ignore_index=True))
    fullreagent_df=(reagent_df.append([reagent_df]*wellcount,ignore_index=True))
    fulltray_df=(tray_df.append([tray_df]*wellcount, ignore_index=True))
    out_df=pd.concat([experiment_df, fullreagent_df, fullrun_df, fulltray_df], axis=1)
#Uncomment the following lines to write this dataframe to a unique CSV
#    with open('%s.csv' %myjson, 'w') as outfile:
#        out_df.to_csv(outfile)
    return(out_df)

#Cleans the dataframe of all rows with empty crystal scores
def clean_df():
    pass

## Unpack logic
    #most granular data for each row of the final CSV is the well information.
    #Each well will need all associated information of chemicals, run, etc. 
    #Unpack those values first and then copy the generated array to each of the invidual wells
def unpackJSON(myjson_fol):
    count=1
    for file in sorted(os.listdir(myjson_fol)):
        if file.endswith(".json"):
            print(file)
            if count <= 1:
                myjson=(os.path.join(myjson_fol, file))
                workflow1_json = json.load(open(myjson, 'r'))
                finaldf=reagentparse(workflow1_json, myjson)
                count+=1
            else:
                myjson=(os.path.join(myjson_fol, file))
                workflow1_json = json.load(open(myjson, 'r'))
                outdf=reagentparse(workflow1_json, myjson)
                finaldf=pd.concat([finaldf,outdf], ignore_index=True)
                count+=1
    #Operations will go here###
    return(finaldf)

def printfinal(myjsonfolder):
    df_out=unpackJSON(myjsonfolder)
    with open('Final.csv', 'w') as outfile:
        print('Complete')
        df_out.to_csv(outfile)

printfinal(myjsonfol)
