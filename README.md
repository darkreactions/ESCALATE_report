Author: Ian Pendleton 
Contact ipendleton .at. haverford.edu (as of September 2018)

Overview
=================
Project Summary:
This code was initially designed as a temporary stop gap between the original version of 'dark reactions project' (DRP) (https://github.com/darkreactions) developed at haverford and the second generation
database for more general distribution. The key challenges addressed were:
  1) Constructing a common intermediate file type (JSON) which has a predictable structure for containerizing individual perovskite experiments
  2) Allowing for flexible workflow development as well as retroactive workflow updating/importing
  3) Process the generated JSON files (each of which describe a set of experiments) into a single final CSV file which describes the entirety of the dataset.
 
#### Summary
This section will be updated in the next version of the code to more fully explain the underlying processes being used.  In general the workflow is as follows:
1. PeroskiteWorkup.py (PW.py) initiates the workflow.
2. PW.py calls CreateDBinput.py
 * CreateDBinput gathers information from the data folder on google drive (https://drive.google.com/open?id=13xmOpwh-uCiSeJn8pSktzMlr7BaPDo7B)
 * CreateDBinput.py generates a normalized JSON for each folder in teh data folder (i.e. each plate of experiments are combined into a single JSON)
 * JSON files are added to FinalizedJSON folder (user created at this point)
3. PW.py calls JSONtoCSV.py to convert the JSONs into a single dataframe
 * dataframe is created through multiple function calls which parse, align and calculate various outputs for the final csv
 * Final.csv is created in the working directory

#### Outside Links
* Specific pydrive API information: https://stackoverflow.com/questions/43865016/python-copy-a-file-in-google-drive-into-a-specific-folder
* Documentation on pydrive: https://github.com/gsuitedevs/PyDrive
* Secure tokens and authentication: https://stackoverflow.com/questions/24419188/automating-pydrive-verification-process


Installation
============
  This build process has been tested on only MacOS High Sierra.  Use at your own risk. 

##### Recommended Installation

If you have not installed conda please do so at the following link: (https://conda.io/docs/user-guide/install/index.html)

1. Create the appropriate environment using the conda builder `conda env create --name expdataworkup -f=experdataworkup_conda_env.yml`

2. Create one additional directory in the working directory:
  * `mkdir FinalizedJSON`

##### Alternative Installation
This installation method assumes that you will be running conda and pip side by side.  Some of the packages required for this code are not available through conda alone.  
To run this software first ensure you have conda installed:(https://conda.io/docs/user-guide/install/index.html)

1. Create new python 3.7 environment in conda: `conda create -n <my_new_env> python=3.7` where "my_new_env" is your preferred environment name.

2. `conda activate <my_new_env>`

3. Install the latest version of the pip package manager, `conda install pip`

4. Using `conda install <package>`
    * numpy, pandas, pylint, xlrd, cython

5. Using `pip install <package>`
    * gspread, pydrive

6. Download the keys.zip file and unzip into the expworkup/creds/ folder. Do not distribute these keys! (https://drive.google.com/file/d/11VP8VazRwFtWHlpEuWdqH0vr_uUQUHV8/view?usp=sharing)
  * Ensure that the files 'client_secrets.json' and 'creds.json' are both present in the 'expworkup/creds/' folder


Running The Code
================

To run the code simply execute the following in the environment created during the installation process:

`python runme.py <my_target_json_folder>`

The code will take time to gather the resources from google, assemble and process the JSON files.  The created JSON files will be deposited into <my_target_json_folder>.  Once the script is finished executing you will see 'Complete' displayed and a 'Final.csv' will be created in the working (base) directory.  

Please email me at ipendleton .at. haverford.edu for questions and to request access to the keys.zip file (contains the necessary authentication to operate our software)!!

Future Development Ideas
========================
1. Update the raw versus final.csv outputs
2. JSON schema to inform parsing
3. class which builds the dataframe from each json
