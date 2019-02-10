#Copyright (c) 2018 Ian Pendleton - MIT License
if __name__ == "__main__":
    import os
    import argparse as ap

    from expworkup import jsontocsv
    from expworkup import createjson
    from expworkup import googleio
    
    # Some command line interfacing to aid in script handling
    parser = ap.ArgumentParser(description='Target Folder')
    parser.add_argument('folder', type=str, help='Please include target folder') 
    parser.add_argument('-d', '--debug', type=int, default=0, help='Turns on testing for implementing new features to the front end of the code, prior to distribution through dataset')
    parser.add_argument('--raw', type=int, default=0, help='final dataframe is printed with all raw values included')
    
    
    args = parser.parse_args()
    myjsonfol = args.folder
    debug = args.debug
    raw = args.raw
    
    
    #debug = args.Debug
    
    #Ensure directories are in order
    if not os.path.exists('data/datafiles'):
        os.mkdir('data/datafiles')
    if not os.path.exists(myjsonfol):
        os.mkdir(myjsonfol)
    
    #run the main body of the code.  Can be called later as a module if needed
    createjson.ExpDirOps(myjsonfol, debug) #Run Primary JSON Creator
    jsontocsv.printfinal(myjsonfol, debug,raw) # RUn the JSON to CSV parser
