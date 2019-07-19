**Authors: Ian Pendleton, Michael Tynes, Aaron Dharna**

**Science Contact:** ipendleton .at. haverford.edu

**Technical Debugging:** mtynes .at. fordham.edu, adharna .at. fordham.edu

Overview
=================
Retrieves experiment files from 4-data folder (the data lake) associated with a given lab and processes to an intermediary
JSON file on users local machine.  The generated JSON files are used to generate a 2d CSV of the data in a format 
compatible with most machine learning software (e.g. SciKit learn).  Additional configuration is required to map the existing
data structures to headers which resemble the users desired configuration.  These mappings are typically trivial for computer
scientists, but may be more challenging for non-domain experts or individuals unfamiliar with manipulating dataframes. The
dataset is augmented with chemical calculations such as concentrations, temperatures derived from models of plate temperature,
and other empirical observations.  In the final steps the dataset is supplemented with chemical features (currently stored in a
CSV as part of this repository) derived from other API such as ChemAxon and RDKit.

A detailed description of the current functionality and logic for this code can be found here: https://docs.google.com/document/d/1vF4mq76mNutCdTCtKAUu91RTm3IS5LX4STxJ0t1JF5U/edit#

User Documents are being updated and can be found here: https://docs.google.com/document/d/1RQJvAlDVIfu19Tea23dLUSymLabGfwJtDnZwANtU05s/edit#heading=h.uzjqm9vtn09j
 
Installation
============
  This build process has been tested on MacOS High Sierra (10.13.5) and Ubuntu Bionic Beaver (18.04)
  
### Pip Install

1. Create new python 3.7 environment in conda: `conda create -n escalate python=3.7`

2. `conda activate escalate`

3. Install the latest version of the pip package manager, `conda install pip`

4. Execute `pip install -r requirements.txt`
   
5. Download the securekey files and move them into the expworkup/creds/ folder. Do not distribute these keys!

6. Ensure that the files 'client_secrets.json' and 'creds.json' are both present in the 'expworkup/creds/' folder

### Custom Environment (Package List)
Install the following python packages prior to use:
- pandas, json, numpy, gspread, pydrive

Please report any failures of the above message to the repo admins
 
Running The Code
=================

Currently supported lab (ids): MIT_PVLab, HC, LBL, ECL, dev

1. Make a new <directory> such as 'mydata'

1. `python runme.py <directory> --lab <your lab id>` 
  
2. For additional command line options see `python runme.py --help`
  
5. A file named <directory>.csv will contain the 2d CSV of the dataset using the configured headers from the data or the mapping developed for the lab.  The `data/` folder will contain the generated JSONs.


Version Data Operation
======================

1. To run this code for versioned data repo file preparation execute the following 
    `python runme.py 2019-02-13_TrainingData -v 1 -s EtNH3Istateset.csv` 

