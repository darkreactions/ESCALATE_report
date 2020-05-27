**Authors: Ian Pendleton, Michael Tynes, Aaron Dharna**

**Science Contact:** jschrier .at. fordham.edu, ian .at. pendletonian.com

**Technical Debugging:** vshekar .at. haverford.edu, gcattabrig .at. haverford.edu, 

## [FAQs](https://docs.google.com/document/d/1RQJvAlDVIfu19Tea23dLUSymLabGfwJtDnZwANtU05s/edit#bookmark=id.8sg0qwagd7yw)
## [Developer Wiki](https://github.com/darkreactions/ESCALATE_Capture/wiki)

Overview
=================
Retrieves experiment files from supported locations and processes to an intermediary JSON file on users local machine.  The generated JSON files are used to generate a 2d CSV of the data in a format 
compatible with most machine learning software (e.g. SciKit learn).  Additional configuration is required to map the existing
data structures to headers which resemble the users desired configuration.  These mappings are typically trivial for computer
scientists, but may be more challenging for non-domain experts or individuals unfamiliar with manipulating dataframes. The
dataset is augmented with chemical calculations such as concentrations, temperatures derived from models of plate temperature,
and other empirical observations.  In the final steps the dataset is supplemented with chemical features and calcs derived from ChemAxon, RDKit, and local datasets saved to this repository.  Additional information on how to control the generation of `_feat_` and `_calc_` columns can be found in the [user documentation here.](https://docs.google.com/document/d/1RQJvAlDVIfu19Tea23dLUSymLabGfwJtDnZwANtU05s/edit#bookmark=id.1shd7vj8nkv8)

[The original ESCALATE publication can be found here.](https://docs.google.com/document/d/1vF4mq76mNutCdTCtKAUu91RTm3IS5LX4STxJ0t1JF5U/edit#)

[User documents, relating to a complete cycle of escalate, can be found here.](https://docs.google.com/document/d/1RQJvAlDVIfu19Tea23dLUSymLabGfwJtDnZwANtU05s/edit#)
 
Installation
============
  This build process has been tested on MacOS High Sierra (10.13.5), MacOS Catalina (10.15.3), Ubuntu Bionic Beaver (18.04), and Windows 10 (version 1909 OS Build 18363.418)

  <b> Windows Users:</b> Please note that while windows has been tested it is not the recommended Operating System. Everything is more challenging, the installation is messier, logging is limited, and the file 
  system interaction is more brittle.

## Mac and Linux
### Initial Setup
#### Pip Install

1. Create new python 3.8 environment in conda and activate: 

   `conda create -n escalate_report python=3.8`

   `conda activate escalate_report`

3. Install the latest version of the pip package manager

   `conda install pip`

4. Then install requirments (still in escalate_report)

    `pip install -r requirements.txt` 

5. Then install conda dependent pieces:

   `conda install -c conda-forge rdkit`

#### Conda Install

1. Execute:

   `conda update conda`

   `conda env create -f environment.yml`

   The `conda env create` command will automatically create an escalate_report environment

#### Custom Environment (Package List)
   <center> <b>Windows Users will likely need to use this </b> </center>

Pip install the following python packages prior to use:
- pandas, json, numpy, gspread, pydrive, cerberus, google-api-python-client==1.7.4, xlrd, xlwt, tqdm, pytest, 

conda install -c conda-forge rdkit

Please report any failures of the above message to the repo admins

### Authentication Setup
   
1. Download the [securekey files](https://www.youtube.com/watch?v=oHg5SJYRHA0) and move them into the root folder (`./`, aka. current working directory, aka. `ESCALATE_report-master/` if downloaded from git). Do not distribute these keys! (Contact a dev for access)

   Note: [Navigate to the wiki for more information on setting up a new lab or generating additional authentication keys](https://github.com/darkreactions/ESCALATE_report/wiki/ONBOARDING-LABS)

2. Ensure that the files 'client_secrets.json' and 'creds.json' are both present in the root folder (`./`, aka. current working directory, aka. `ESCALATE_report-master/` if downloaded from git).  The correct folder for these keys is the one which contains the runme.py script.

3. Stop here if you don't want to use ChemAxon for feature generation. Rdkit and the available ESCALATE features will still be generated. 
   
   * Note: ESCALATE will throw warnings if chemaxon features are implemented in  `type_command.csv`, these can be ignored if that is the desired functionality

#### Optional for ChemAxon Support
4. Download and [install ChemAxon JChemSuite](https://chemaxon.com/products/jchem-engines) and obtain a [ChemAxon License Free for academic use](https://academia.chemaxon.com/)

5. Follow the installation instruction found on [ChemAxons website](https://docs.chemaxon.com/display/docs/Installation+Guide.) Be sure to not the location of the JChemSuite installation (i.e. `~/opt/chemaxon/jchemsuite/bin` on linux or `/Applications/JChemSuite/bin/` on MacOSX)

   * There are also docs on license install using a graphical user interface (GUI) here: https://docs.chemaxon.com/display/docs/Licenses.html

5. You will need to specify the location of your chemaxon installation locations in `./expworkup/devconfig.py` at the bottom of the file. 
 
Running The Code
=================
Currently supported `google_drive_target_name` (user defined folder names): 
  * **MIT Data:** MIT_PVLab
  * **HC and LBL Data:** 4-Data-WF3_Iodide, 4-Data-WF3_Alloying, 4-Data-Bromides, 4-Data-Iodides
  * **Development:** dev

## Basic Overview
A more detailed instruction manual including videos overviewing how to operated the code can be found in the [ESCALATE user manual](https://docs.google.com/document/d/1RQJvAlDVIfu19Tea23dLUSymLabGfwJtDnZwANtU05s/edit?usp=sharing)

__Definitions__

`<my_local_folder>`: is the name of the folder where files should be created.  _This will be automatically created by ESCALATE_report if it does not exist._  The specified name will also be used as the final exported csv (i.e. if <my_local_folder> is perovskitedata, perovskitedata.csv will be generate)

`<google_drive_target_name>`: one or more of the available datasets. see examples below


1. You can always get runtime information by executing:

    `python runme.py --help`

2. To execute a normal run with chemaxon, rdkit, and ESCALATE calcs (see installation instructions above for more details)

   `python runme.py <my_local_folder> -d <google_drive_target_name>`
  
3. To improve the clarity of column headers specify them in the `dataset_rename.json` file.  All columns can be viewed in the initial run by executing: 

   `python runme.py <my_local_folder> -d <google_drive_target_name> --raw 1`

4. __Columns that do not conform to the `_{category}_` (e.g., `_feat_`, `_rxn_`) will be omitted unless `--raw 1` is enabled!__ 
   * A list of the columns not conforming to the naming scheme will be exported to './<my_local_folder>/logging/UNNAMED_REPORT_COLUMNS.txt'.  
   * The USER can specify an appropriate name in dataset_rename.json
   * To see all columns with naming directly from datasource use: `--raw 1`
   * __Conflicting namespaces will be purged!__

4. Significant flexibility is enabled for `_feat_` (via, type_command.csv) and `_calc_` (via, ./utils/calc_command.py) specification.  [For examples, discussion, and limitations of these specifications please see the USER docs.](https://docs.google.com/document/d/1RQJvAlDVIfu19Tea23dLUSymLabGfwJtDnZwANtU05s/edit#bookmark=id.1shd7vj8nkv8)
   * `_calc_` generation can be skipped by using the `--disablecalcs True` flag on the CLI
   * To speed up calc and feature development the first portion of the code can be skipped by:
      1. Running the code with `--offline 1` 
      2. After the first iteration completes running future instances with `--offline 2`
  
5. A file named `<my_local_folder>.csv` will contain the 2d CSV of the dataset using the configured headers from the data or the mapping developed for the lab.  The `data/` folder will contain the generated JSONs.

6. Intermediate dataframes can be exported in bulk by specifying:

   `python runme.py <my_local_folder> -d <google_drive_target_name> --debug 1`

To add additional target directories please see the how-to guide [here](https://github.com/darkreactions/ESCALATE_report/wiki/ONBOARDING-LABS).  If you would like to add these to the existing datasets, please issue a git merge request after you add the necessary information.

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

Example Useage
==============

* `python runme.py 4-Data-Iodides -d 4-Data-Iodides`

* `python runme.py 4-Data-Iodides -d 4-Data-Iodides 4-Data-WF3_Iodide 4-Data-WF3_Alloying`

* `python runme.py dev -d dev --debug 1 --raw 1 --offline 1`

* `python runme.py perovskitedata -d 4-Data-Iodides --verdata 0111 --state example.csv`

FAQs, Trouble Shooting, and Tutorials
======================
1. [FAQs](https://github.com/darkreactions/ESCALATE_Capture/wiki/Users:-FAQs)
2. Trouble Shooting Help: please send log file, any terminal output and a brief explanation to ipendlet .at. haverford.edu for help. 
3. Tutorials
   1. [Adding a new target for data workup](https://github.com/darkreactions/ESCALATE_Capture/wiki/Developers:-Adding-New-Labs-to-devconfig.py:--Capture-and-Report)
   2. [Adding a new target for experiment generation]()
