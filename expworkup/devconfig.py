import platform
import os
import sys

cwd = os.getcwd()

#######################################
# version control todo: ian where does this get used? Not sure if we should have to manually do this
RoboVersion = 2.5

SUPPORTED_LABS = ['LBL', 'HC', 'MIT_PVLab', 'ECL']

#######################################
# chemistry-relevant specifications

max_robot_reagents = 7
maxreagentchemicals = 4
volspacing = 50  # reagent microliter (uL) spacing between points in the stateset

# perovskite solvent list (simple specification of what is a liquid)
# assumes only 1 liquid / reagent
solventlist = ['GBL', 'DMSO', 'DMF', 'DCM', 'CBz']

#######################################
# Lab-specific variables

lab_vars = {
    'MIT_PVLab':
        {
            'template_folder': '1PVeVpNjnXiAuzm3Oq2q-RiiLBhKPGW53',
            'targetfolder': '1tUb4GcF_tDanMjvQuPa6vj0n9RNa5IDI',  # target folder for run generation
            'chemsheetid': '1htERouQUD7WR2oD-8a3KhcBpadl0kWmbipG0EFDnpcI',
            'chem_workbook_index': 0,
            'reagentsheetid': '1htERouQUD7WR2oD-8a3KhcBpadl0kWmbipG0EFDnpcI',
            'reagent_workbook_index': 1,
            'reagent_interface_amount_startrow': 16,
            'max_reagents': 8,  # todo: discuss
            'reagent_alias': 'Precursor'
        },
    'HC':
        {
            'template_folder': '131G45eK7o9ZiDb4a2yV7l2E1WVQrz16d',
            'targetfolder': '11vIE3oGU77y38VRSu-OQQw2aWaNfmOHe',  # target folder for new experiments
            'chemsheetid': '1JgRKUH_ie87KAXsC-fRYEw_5SepjOgVt7njjQBETxEg',
            'chem_workbook_index': 0,
            'reagentsheetid': '1JgRKUH_ie87KAXsC-fRYEw_5SepjOgVt7njjQBETxEg',
            'reagent_workbook_index': 1,
            'reagent_interface_amount_startrow': 15,
            'reagent_alias': 'Reagent'
        },
    'LBL':
        {
            'template_folder': '131G45eK7o9ZiDb4a2yV7l2E1WVQrz16d',
            'targetfolder': '11vIE3oGU77y38VRSu-OQQw2aWaNfmOHe',  # target folder for new experiments
            'chemsheetid': '1JgRKUH_ie87KAXsC-fRYEw_5SepjOgVt7njjQBETxEg',
            'chem_workbook_index': 0,
            'reagentsheetid': '1JgRKUH_ie87KAXsC-fRYEw_5SepjOgVt7njjQBETxEg',
            'reagent_workbook_index': 1,
            'reagent_interface_amount_startrow': 15,
            'reagent_alias': 'Reagent',
            'Robofile': '_RobotInput.xls'
        },
    'dev':
        {
            'template_folder': '1w5tReXSRvC6cm_rQy74-10QLIlG7Eee0',
            'targetfolder': '19nt2-9Inub8IEYDxOLnplCPDEYt1NPqZ', # this is 1 dev
            'chemsheetid': '1uj6A3TH2oMSQwzhPapfmr1t-CbevEGmQjKIyfg9aSgk',
            'chem_workbook_index': 0,
            'reagentsheetid': '1uj6A3TH2oMSQwzhPapfmr1t-CbevEGmQjKIyfg9aSgk',
            'reagent_workbook_index': 1,
            'reagent_interface_amount_startrow': 16,
            'reagent_alias': 'Reagent',
            'max_reagents': 8,
            'required_files': ['observation_interface', 'preparation_interface', 'metadata.json'],
            'observation_interface': {'uid_col': 'F'}
        }
}
