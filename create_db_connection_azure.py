import os
from sqlalchemy import create_engine
from urllib.parse import quote_plus  # Importação necessária para o quote_plus
from dotenv import load_dotenv
import logging

# Carregar variáveis de ambiente
load_dotenv()

def create_db_connection(username, password, host, port, database):
    "Cria a conexão com o banco de dados Azure SQL Database usando SQLAlchemy e ODBC."
    try:
        # Escapar username e password para evitar problemas com caracteres especiais
        escaped_username = quote_plus(username)
        escaped_password = quote_plus(password)

        # Formatar a string de conexão para Azure SQL Database
        connection_string = (
            f"mssql+pyodbc://{escaped_username}:{escaped_password}@"
            f"{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
        )

        # Criar a engine do SQLAlchemy
        engine = create_engine(connection_string)

        # Log para confirmar a conexão
        logging.info(f"Connected to Azure SQL Database {database} on {host}")
        
        return engine
    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
        raise
