import logging

def setup_logger(logger_name, log_file, level=logging.INFO, stream=False):
    l = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(asctime)s> %(name)s ; %(levelname)s - %(message)s')
    fileHandler = logging.FileHandler(log_file, mode='w')
    fileHandler.setFormatter(formatter)

    l.setLevel(level)
    l.addHandler(fileHandler)

    if stream:
        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(formatter)
        l.addHandler(streamHandler)   