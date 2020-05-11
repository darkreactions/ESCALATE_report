**Authors: Ian Pendleton, Michael Tynes, Aaron Dharna**

**Science Contact:** jschrier .at. fordham.edu, ian .at. pendletonian.com

**Technical Debugging:** vshekar .at. haverford.edu, gcattabrig .at. haverford.edu, 

## [FAQs](https://github.com/darkreactions/ESCALATE_Capture/wiki/Users:-FAQs)
## [Wiki](https://github.com/darkreactions/ESCALATE_Capture/wiki)

Overview
=================
Retrieves experiment files from supported locations and processes to an intermediary JSON file on users local machine.  The generated JSON files are used to generate a 2d CSV of the data in a format 
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
  This build process has been tested on MacOS High Sierra (10.13.5), MacOS Catalina (10.15.3), Ubuntu Bionic Beaver (18.04), and Windows 10 (version 1909 OS Build 18363.418)

  <b> Windows Users:</b> Please note that while windows has been tested it is not the recommended Operating System. Everything is more challenging, the installation is messier, logging is limited, and the file 
  system interaction is more brittle.

## Mac and Linux
### Pip Install

1. Create new python 3.8 environment in conda: `conda create -n escalate python=3.8`

2. `conda activate escalate`

3. Install the latest version of the pip package manager, `conda install pip`

4. Execute:

   `pip install -r requirements.txt` 

   OR 

   `conda create --name <your_env_name_here> --file conda_requirements.txt`

5. Execute `conda install -c conda-forge rdkit`
   
5. Download the [securekey files](https://www.youtube.com/watch?v=oHg5SJYRHA0) and move them into the expworkup/creds/ folder. Do not distribute these keys! (Contact a dev for access)

6. Ensure that the files 'client_secrets.json' and 'creds.json' are both present in the main directory (directly in ESCALATE_report)

6. Stop here if you don't want to use the automated feature generation. You can specify the simple workup from google by executing: 

   `python runme.py <my_local_folder> -d <google_drive_target_name> --simple 1` 

6. Download and [install ChemAxon JChemSuite](https://chemaxon.com/products/jchem-engines) and obtain a [ChemAxon License Free for academic use](https://academia.chemaxon.com/)

7. You will need to specify the location of your chemaxon installation locations in `./expworkup/devconfig.py` at the bottom of the file

## Windows

### Custom Environment (Package List)
Pip install the following python packages prior to use:
- pandas, json, numpy, gspread, pydrive, cerberus, google-api-python-client==1.7.4, xlrd, xlwt, tqdm, pytest, 

conda install -c conda-forge rdkit

Please report any failures of the above message to the repo admins
 
Running The Code
=================
Currently supported `google_drive_target_name` (user defined folder names): 
  * **MIT Data:** MIT_PVLab
  * **HC and LBL Data:** 4-Data-WF3_Iodide, 4-Data-WF3_Alloying, 4-Data-Bromides, 4-Data-Iodides
  * **Development:** dev

## Basic Overview
A more detailed instruction manual including videos overviewing how to operated the code can be found in the [ESCALATE user manual](https://docs.google.com/document/d/1RQJvAlDVIfu19Tea23dLUSymLabGfwJtDnZwANtU05s/edit?usp=sharing)

1. You can always get runtime information by executing:

    `python runme.py --help`

1. To execute a simple run with no data augmentation:
   
   `python runme.py <my_local_folder> -d <google_drive_target_name> --simple 1` 

2. To execute a normal run with chemaxon, rdkit, and ESCALATE calcs (see installation instructions above for more details)

   `python runme.py <my_local_folder> -d <google_drive_target_name>`
  
3. To improve the clarity of column headers specify them in the `dataset_rename.json` file.  All columns can be viewed in the initial run by executing: 

   `python runme.py <my_local_folder> -d <google_drive_target_name> --raw 1`

4. __Columns that do not conform to the `_{category}_` (e.g., `_feat_`, `_rxn_`) will be omitted unless `--raw 1` is enabled!__
  
5. A file named `<my_local_folder>.csv` will contain the 2d CSV of the dataset using the configured headers from the data or the mapping developed for the lab.  The `data/` folder will contain the generated JSONs.

6. Intermediate dataframes can be exported in bulk by specifying:

   `python runme.py <my_local_folder> -d <google_drive_target_name> --debug 1`

To add additional target directories please see the how-to guide [here](https://github.com/darkreactions/ESCALATE_Capture/wiki/Developers:-Adding-New-Labs-to-devconfig.py).  If you would like to add these to the existing datasets, please issue a git merge request after you add the necessary information.

## Report to Versioned Data to ESCALATion
More detailed instructions can be found in the [ESCALATE user manual](https://docs.google.com/document/d/1RQJvAlDVIfu19Tea23dLUSymLabGfwJtDnZwANtU05s/edit?usp=sharing).

**If you are using Windows10 please follow [these instructions](https://github.com/darkreactions/ESCALATE_Capture/wiki/User:-Configuring-Windows-Environment) on what you will need to setup your environment. Consider using Ubuntu or wsl instead!**

1. Ensure that [versioned data repo](https://gitlab.sd2e.org/sd2program/versioned-datasets) and [escalation](http://escalation.sd2e.org/) are installed

2. Create an issue on [versioned repo with new `crank-number`](https://gitlab.sd2e.org/sd2program/versioned-datasets/issues)

3. `python runmy.py <my_local_folder> -d <google_drive_target_name> -v <crank-number>`

4. This will generate files for upload to versioned data repo with the names:
   * <`crank-number`>.<`dataset-name`>.csv
   * <`crank-number`>.<`dataset-name`>.index.csv  

5. Move these files to the `/pathto/versioned-dataset/data/perovskite/<my_local_folder>`

6. Follow Readme.md instructions for versioned=datasets

## Include a `state-set` file with Crank

1. Obtain a [`stateset`](https://drive.google.com/open?id=1HrVSv9DN7vJCKNVZ-y4yiwg6vAXxBBw8) or generate a [`stateset`](https://github.com/darkreactions/ESCALATE_Capture)

2. `python runmy.py <my_local_folder> -d <google_drive_target_name> -v <crank-number> -s <state-set_file_name.csv>`

3. Follow 5-6 above


Use Examples
======================

1. Generate the latest copy of a target data folder
2. Generate versioned repo crank with state set
   * `python runme.py 2019-02-13_TrainingData -v 1 -s EtNH3Istateset.csv` 
3.

FAQs, Trouble Shooting, and Tutorials
======================
1. [FAQs](https://github.com/darkreactions/ESCALATE_Capture/wiki/Users:-FAQs)
2. Trouble Shooting Help: please send log file, any terminal output and a brief explanation to ipendlet .at. haverford.edu for help. 
3. Tutorials
   1. [Adding a new target for data workup](https://github.com/darkreactions/ESCALATE_Capture/wiki/Developers:-Adding-New-Labs-to-devconfig.py:--Capture-and-Report)
   2. [Adding a new target for experiment generation]()
