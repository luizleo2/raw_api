import logging
import os

def setup_logging(log_filename):
    # Ensure the logs directory exists
    logs_dir = 'logs'
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    
    # Set the full path for the log file
    log_filepath = os.path.join(logs_dir, log_filename)
    
    # Criar o logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Criação de um manipulador de arquivo
    file_handler = logging.FileHandler(log_filepath)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', '%Y-%m-%d %H:%M:%S'))
    
    # Criação de um manipulador para o console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', '%Y-%m-%d %H:%M:%S'))
    
    # Adiciona os manipuladores ao logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
