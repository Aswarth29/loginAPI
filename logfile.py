import logging, sys
from os import path


def log_insert(level, format, text):
    infoLog = logging.FileHandler("log.log")
    infoLog.setFormatter(format)
    logger = logging.getLogger("log.log")
    logger.setLevel(level)

    if not logger.handlers:
        logger.addHandler(infoLog)
        if (level == logging.INFO):
            logger.info(text)
        if (level == logging.ERROR):
            logger.error(text)
        if (level == logging.WARNING):
            logger.warning(text)

    infoLog.close()
    logger.removeHandler(infoLog)

    return


    
    #except Exception as e:
        #exception_type, exception_object, exception_traceback = sys.exc_info()
        #filename = exception_traceback.tb_frame.f_code.co_filename
        #line_number = exception_traceback.tb_lineno,exception_traceback 
        #log_message = path.basename(filename) + str(line_number) 
        #formatLOG = logging.Formatter( 
            #'%(asctime)s, %(levelname)s , %(message)s', "%m/%d/%Y %H:%M:%S" )
        #log_insert(logging.ERROR,formatLOG,log_message)'''


# logfile = sys.argv[1]  # Log File Name
# formatLOG = logging.Formatter(
#     "%(asctime)s, %(levelname)s , %(message)s,
#     "%m/%d/%Y %H:%M:%S")
# Log_insert(logfile, logging.INFO, formatLOG,)


