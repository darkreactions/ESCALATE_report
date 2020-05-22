RELEASE HISTORY
===============
### Be sure to update the version number in 'runme.py'!

1.1 (2020-05-20)
-----------------
  * Added calc_command.json support
  * Streamlined feature specification
  * Improved error handling

1.0 (2020-04-10)
-----------------
  * Integrated chemdescriptor module
  * Generalized feature report around type_command.csv table
  * Added full dataframe export option for v3 ETL targets, and additional export options
  * Create a default view of the generated datasets (resembles past versions)
  * Significant improvements to generalizability, flexibility, readability, and inspectability of code
  * Generalized 'chemical_type' specification with user level access
  * Updated Docstrings
  * Added Test Kit, nothing fancy, but it is a (messy) start
  * Added additional schema representations to cerberus (validated datasets, with significant improvements to dataset integrity)

0.86 (2020-03-30)
-----------------
  * separated baseline parsing from all calcs and feats
  * added dev test on baseline parseing
  * debug now exports dataframes at every step
  * all columns exported as lowercase (as much as possible)

0.85 (2020-03-27)
------------------------
  * Wrapped all authentication, only activates when needed
  * Added support for multi-folder, multi-lab data collation
  * Improved lab vs. dataset distinction (lab is where capture runs, datasets are targets for report code)
  * Documentation, docstrings, code readability changes

0.84 (2020-02-17)
------------------------
  * Streamline workflow for gdrive and gspread authentication, less errors
  * Deep testing with windowsOS for report functionality
  * Updated creds handling

0.83 (2020-02-09)
------------------------
  * Fixed windows failures (unicode parsing, and file handling issues)
  * Updated documentation (incorporated windows install and use cases)
  * Improved logging

0.82 (2020-01-17)
-------------------------
  * Code cleaning, synchronizing between capture and report devconfig, updates to readability
  * Implemented reagent_object export (Warning! only tested with wf 1.1 LBL/HC ONLY)
  * Implemented reagent_object nominals / actuals observables reporting in unique csv files
  * Expert curated features added as default for LBL

0.8.1 (2019-10-30)
-------------------------
  * Changed the default header `_rxn_M_*` to target v1 concentrations
  * Shifted deprecated v0 from `_rxn_M_*` to `_raw_v0-M_* 'concentrations'
  * Updated perov_des_edited.csv 

0.8 (2019-07-22)
-------------------------
  * Added support for MIT_PVLab workflow
  * Update CLI: added choice for lab
  * Generalized interface parsing to handle sheets from both labs 
  * Create development folder for debug mode with examples from both labs (HC/LBL and MIT)

0.7 (2019-06-17)
-------------------------
  * New concentration `_rxn_v1-M_` parameter added (using this notation until Alex C finalizes work)
  * Added prototype output support
  * various bug fixes

0.6 (2019-04-08)
--------------------------
  * Support for multiple experiments / tray processing
  * Support for versioned data repo file generation 
  * Preliminary support for escalation state set upload / versioned data upload

0.5 (2019-02-12)
--------------------------
  * Thorough logging and output beautification
  * Modularized code for escalation work
  * improved googleio interfacing --> improves performance

0.4 (2019-01-10)
--------------------------
  * Added debugging for future developement
  * Updated logging

0.3.1 (2018-11-16)
--------------------------
  * Fixed headers for new temperature feature, added cli for raw data dump

0.2 (2018-10-01)
--------------------------
  * Organized code for distribution

0.1 (2018-09-16)
----------------
  * Commited first fully functioning version of the experiment to CSV pipeline
  * Updated associated files for logging and running code
