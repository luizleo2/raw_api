import os
from dotenv import load_dotenv

# Carregar as variáveis do arquivo .env
load_dotenv()

def get_db_config():
    """Carrega as configurações do banco de dados a partir do .env, com verificação de erros"""
    # Carregar variáveis de ambiente
    config = {
        'username': os.getenv('DB_USERNAME'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT', 1433),  # Usando 1433 como valor padrão para o Azure SQL
        'raw_database': os.getenv('RAW_DATABASE'),
        'report_database': os.getenv('REPORT_DATABASE')
    }
    
    # Verificar se todas as variáveis essenciais estão presentes
    for key, value in config.items():
        if value is None:
            raise ValueError(f"A variável de ambiente '{key}' não foi definida no arquivo .env")
    
    return config

# Exemplo de uso da função de configuração
if __name__ == "__main__":
    try:
        db_config = get_db_config()
        print("Configurações do banco de dados carregadas com sucesso:")
        print(db_config)
    except ValueError as e:
        print(f"Erro: {e}")
