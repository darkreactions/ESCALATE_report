RELEASE HISTORY
===============

0.8 (2018-10-30)
-------------------------
  * Changed the default header `_rxn_M_*` to target v1 concentrations
  * Shifted deprecated v0 from `_rxn_M_*` to `_raw_v0-M_* 'concentrations'
  * Updated perov_des_edited.csv 

0.8 (2019-07-22)
-------------------------
  * Added support for MIT_PVLab workflow
  * Generalized interface parsing
  * improved flexibility for reading various columns from observation_interface(old crystal_score form)
  * specific development folder with examples from both labs (HC/LBL and MIT)

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