Author: Ian Pendleton 
Contact ipendleton .at. haverford.edu (as of February 2019)

Overview
=================
Retrieves experiment files (including template, models and objects) from data lake and processes to an intermediary
JSON file.  The generated JSON files are used to generate a 2d CSV of the data in a format compatible with most 
machine learning software (e.g. SciKit learn).  The dataset is augmented with chemical calculations such as concentrations, 
temperatures derived from models of plate temperature, and other empirical observations.  In the final steps the dataset 
is supplemented with chemical features (currently stored in a CSV as part of this repository) derived from other API 
such as ChemAxon and RDKit.
 
#### Operation Summary
1. `python runme.py <directory>` 
  
2. For debugging:  `python runme.py <directory> --debug 1` (Debug targets the 1-dev/debug folder on GDrive
  
3. GDrive API will pull chemical information displaying: "Obtaining chemical information from Google Drive...done" 

4. Operation is completed when "Complete" is displayed

5. Final.csv will contain the 2d CSV of the dataset.  The `data/` folder will contain the generated JSONs.

Installation
============
  This build process has been tested on MacOS High Sierra (10.13.5) and Ubuntu Bionic Beaver (18.04)
  
### Conda Installation
If you have not installed conda please do so at the following link: (https://conda.io/docs/user-guide/install/index.html)

1. Create the appropriate environment using the conda builder `conda env create --name escalate -f escalate_environment.yml`

2. Download the secure keys (https://drive.google.com/open?id=11VP8VazRwFtWHlpEuWdqH0vr_uUQUHV8) and them to expworkup/creds/ folder


### Pip Install

1. Create new python 3.7 environment in conda: `conda create -n escalate python=3.7`

2. `conda activate escalate`

3. Install the latest version of the pip package manager, `conda install pip`

4. Execute `pip install -r requirements.txt`
   
5. Download the keys.zip file and unzip into the expworkup/creds/ folder. Do not distribute these keys! (https://drive.google.com/file/d/11VP8VazRwFtWHlpEuWdqH0vr_uUQUHV8/view?usp=sharing)
6. Ensure that the files 'client_secrets.json' and 'creds.json' are both present in the 'expworkup/creds/' folder

### Custom Environment (Package List)
Install the following python packages prior to use:
- pandas, json, numpy, gspread, pydrive

Please report any failures of the above message to the repo admins

----------------

Please email me at ipendleton .at. haverford.edu for questions and to request access to the keys.zip file 
(contains the necessary authentication to operate our software)!!

----------------

##### Outside Links
* Specific pydrive API information: https://stackoverflow.com/questions/43865016/python-copy-a-file-in-google-drive-into-a-specific-folder
* Documentation on pydrive: https://github.com/gsuitedevs/PyDrive
* Secure tokens and authentication: https://stackoverflow.com/questions/24419188/automating-pydrive-verification-process
