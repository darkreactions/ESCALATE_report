RELEASE HISTORY
===============

0.1 (2018-09-16)
----------------
  * Commited first fully functioning version of the experiment to CSV pipeline
  * Updated associated files for logging and running code

0.2 (2018-10-01)
--------------------------
  * Organized code for distribution

0.3 (2018-11-16)
--------------------------
  * Added compatibility to render 7 reagents to json files
  * Parsing for various numbers of reagents
  * CSV generation for 7 reagents (though the concentrations of organic, inorganic, acid will need generalization)
  * Data process for recording actual temperature was added
  * Created a folder for tests and debugging of new features for the pipeline prior to implementation
    * Still need a test kit!

0.3.1 (2018-11-16)
--------------------------
  * Fixed headers for new temperature feature, added cli for raw data dump

0.4 (2019-01-10)
--------------------------
  * Added debugging for future developement
  * Updated logging

0.5 (2019-02-12)
--------------------------
  * Thorough logging and output beautification
  * Modularized code for escalation work
  * improved googleio interfacing --> improves performance

0.6 (2019-04-08)
--------------------------
  * Support for multiple experiments / tray processing
  * Support for versioned data repo file generation 
  * Preliminary support for escalation state set upload / versioned data upload
