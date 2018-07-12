# ExpDataWorkup

Overview of installation and operation:
conda install xlrd
conda install pandas

pip install gspread
pip install pydrive

2) request from admin client_secrets.json key or access it on the SD2 google drive.  Do not distribute this key!


Preparing the relevant files and directories:
1) This program does not write to google drive in any way, only reads files and generates local copies of "THE JSON" file (tm). If
    files are not appropriately named or formatted in ALL relevant directories this code will not run.  This script relies on the
    assumption that all files in the "data" directory on google drive have been appropriately generated and curated.
2) 

Please email me at ipendleton .at. haverford.edu for questions and to request the client_secrets.json key!!

Project Summary:
This toolset was initially designed as a temporary stop gap between the work version of 'dark reactions project' (DRP) at haverford and the addition
of new generalized reaction workup for importing into DRP database. The key challenges addressed were:
  1) Constructing a common intermediate file type which has a predictable structure
  2) Allowing for flexible workflow development as well as retroactive workflow updating/importing (still not sure how to do this) 

Future development: