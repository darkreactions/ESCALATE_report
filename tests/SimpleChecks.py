#%%
import pandas as pd

df = pd.read_csv('./ESCALATE_report/perovskitedata.csv', skiprows=0)
df2 = pd.read_csv('./versioned-datasets/data/perovskite/perovskitedata/0037.perovskitedata.csv', skiprows=4)

df.rename(columns = {'Unnamed: 2': '_raw_placeholder'}, inplace=True)
df.rename(columns = {'Bulk Actual Temp (C)': '_rxn_temperatureC_actual_bulk'}, inplace=True)
df.rename(columns = {'Crystal Score': '_out_crystalscore'}, inplace=True)
df.rename(columns = {'_out_predicted': '_raw_model_predicted'}, inplace=True)




list(df2)
#%%
list(df)
