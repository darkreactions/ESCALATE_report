import json
import pandas as pd
from pandas.io.json import json_normalize

with open('JSONFiles/baby.json', 'r') as f_in:
    f=json.load(f_in)
    df=json_normalize(f)
#    df=pd.read_json(f)
    print(df)

