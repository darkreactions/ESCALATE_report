import sys
import logging

def mylogfunc(args):
    logger = logging.getLogger('report')
    logger.setLevel(logging.DEBUG)
    # create file handler which logs event debug messages
    fh = logging.FileHandler('%s_LogFile.log'%args.workdir)
    fh.setLevel(logging.DEBUG)
    # create console handler with a higher log level
    wh = logging.StreamHandler()
    wh.setLevel(logging.WARN)
    # create error formatter with the highest log level
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s> %(name)s ; %(levelname)s - %(message)s')
    warning_formatter = logging.Formatter('%(asctime)s> %(name)s ; %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    wh.setFormatter(warning_formatter)
    logger.addHandler(fh)
    logger.addHandler(wh) 
    return('%s_Logfile.log'%args.workdir)