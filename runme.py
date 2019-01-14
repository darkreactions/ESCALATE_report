#Copyright (c) 2018 Ian Pendleton - MIT License
if __name__ == "__main__":
    from expworkup import jsontocsv
    from expworkup import createjson
    import argparse as ap
    import os
    from expworkup import googleio
    
    # Some command line interfacing to aid in script handling
    parser = ap.ArgumentParser(description='Target Folder')
    parser.add_argument('folder', type=str, help='Please include target folder') 
    parser.add_argument('-d', '--debug', type=int, default=0, help='Turns on testing for implementing new features to the front end of the code, prior to distribution through dataset')
    
    args = parser.parse_args()
    myjsonfol = args.folder
    debug = args.debug
    
    #debug = args.Debug
    
    #Ensure directories are in order
    if not os.path.exists('data/datafiles'):
        os.mkdir('data/datafiles')
    if not os.path.exists(myjsonfol):
        os.mkdir(myjsonfol)
    
    #run the main body of the code.  Can be called later as a module if needed
    createjson.ExpDirOps(myjsonfol, debug) #Run Primary JSON Creator
    jsontocsv.printfinal(myjsonfol, debug) # RUn the JSON to CSV parser
