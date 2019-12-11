#%%
import pandas as pd 
import os

#%%
WORKINGDIR = '/Users/ipendleton/Dropbox/ESCALATE/ESCALATE_report/data/'

desc_list = ['ExpertCurated_20191121.csv', 'perov_desc_20191104.csv']

expert = os.path.join(WORKINGDIR, desc_list[0])
perov = os.path.join(WORKINGDIR, desc_list[1])

df_1 = pd.read_csv(expert)
df_2 = pd.read_csv(perov)
df_2.set_index('_raw_inchikey', inplace=True)
df_1.rename(columns={'InChiKey': '_raw_inchikey'}, inplace=True)
df_1.set_index('_raw_inchikey', inplace=True)

#%%
df_out = df_2.join(df_1)
df_out.drop(labels=['smiles', 'new_smiles'], axis=1)
df_out.rename(columns={'standardized_smiles': '_raw_standardized_smiles'}, inplace=True)
df_out.to_csv('perov_desc_edited2.csv')


# %%
