from flask import Flask, request, jsonify
import pandas as pd
from create_db_connection_azure import create_db_connection
from get_db_config_azure import get_db_config
from dotenv import load_dotenv
import logging 
from logging_config import setup_logging 
from raw_generator import generate_products_data, generate_reviews_data, get_existing_product_ids
from sqlalchemy import text
from flask_cors import CORS
import pyodbc

# Carregar variáveis de ambiente
load_dotenv()

# Setup do logging
setup_logging(log_filename='apix.log')

app = Flask(__name__)
CORS(app)  # Isso permite requisições de qualquer origem

# Conectar aos bancos de dados de origem e destino usando a função get_db_config
def get_engines():
    config = get_db_config()  # Aqui estamos pegando as configurações do banco diretamente da função
    raw_engine = create_db_connection(config['username'], config['password'], config['host'], config['port'], config['raw_database'])
    report_engine = create_db_connection(config['username'], config['password'], config['host'], config['port'], config['report_database'])
    return raw_engine, report_engine

# Extrair dados usando uma consulta SQL e retornar um DataFrame
def extract_data(engine, query, table_name):
    logging.info(f"Starting data extraction from table '{table_name}'")
    df = pd.read_sql(query, engine)
    logging.info(f"Data extracted from table '{table_name}' successfully")
    return df


@app.route('/products', methods=['POST'])
def add_product():
    try:
        # Recebe os dados da requisição
        data = request.get_json()

        # Verifica se o payload é uma lista de produtos ou um único produto
        if isinstance(data, list):
            # Caso seja uma lista de produtos
            for product in data:
                name = product['name']
                price = product['price']
                stock = product['stock']

                # Conectar ao banco
                raw_engine, _ = get_engines()

                query = text("""
                    INSERT INTO products (name, price, stock)
                    VALUES (:name, :price, :stock)
                """)

                with raw_engine.connect() as connection:
                    transaction = connection.begin()
                    try:
                        connection.execute(query, {
                            "name": name,
                            "price": price,
                            "stock": stock
                        })
                        transaction.commit()
                    except Exception as e:
                        transaction.rollback()
                        raise e

            return jsonify({"message": "Products added successfully"}), 201

        else:
            # Caso seja um único produto
            name = data['name']
            price = data['price']
            stock = data['stock']

            # Conectar ao banco
            raw_engine, _ = get_engines()

            query = text("""
                INSERT INTO products (name, price, stock)
                VALUES (:name, :price, :stock)
            """)

            with raw_engine.connect() as connection:
                transaction = connection.begin()
                try:
                    connection.execute(query, {
                        "name": name,
                        "price": price,
                        "stock": stock
                    })
                    transaction.commit()
                except Exception as e:
                    transaction.rollback()
                    raise e

            return jsonify({"message": "Product added successfully"}), 201

    except Exception as e:
        logging.error(f"Error adding product: {str(e)}")
        return jsonify({"error": "Error adding product"}), 500

@app.route('/reviews', methods=['POST'])
def add_review():
    try:
        # Recebe os dados da requisição
        data = request.get_json()

        # Verifica se o payload é uma lista de reviews ou um único review
        if isinstance(data, list):
            # Caso seja uma lista de reviews
            for review in data:
                product_id = review['product_id']
                rating = review['rating']
                review_text = review.get('review_text', "")  # Campo opcional

                # Conectar ao banco
                raw_engine, _ = get_engines()

                query = text("""
                    INSERT INTO reviews (product_id, rating, review_text)
                    VALUES (:product_id, :rating, :review_text)
                """)

                with raw_engine.connect() as connection:
                    transaction = connection.begin()
                    try:
                        connection.execute(query, {
                            "product_id": product_id,
                            "rating": rating,
                            "review_text": review_text
                        })
                        transaction.commit()
                    except Exception as e:
                        transaction.rollback()
                        raise e

            return jsonify({"message": "Reviews added successfully"}), 201

        else:
            # Caso seja um único review
            product_id = data['product_id']
            rating = data['rating']
            review_text = data.get('review_text', "")  # Campo opcional

            # Conectar ao banco
            raw_engine, _ = get_engines()

            query = text("""
                INSERT INTO reviews (product_id, rating, review_text)
                VALUES (:product_id, :rating, :review_text)
            """)

            with raw_engine.connect() as connection:
                transaction = connection.begin()
                try:
                    connection.execute(query, {
                        "product_id": product_id,
                        "rating": rating,
                        "review_text": review_text
                    })
                    transaction.commit()
                except Exception as e:
                    transaction.rollback()
                    raise e

            return jsonify({"message": "Review added successfully"}), 201

    except Exception as e:
        logging.error(f"Error adding review: {str(e)}")
        return jsonify({"error": "Error adding review"}), 500

    


# Função para inserir uma nova venda
@app.route('/sales', methods=['POST'])
def add_sale():
    try:
        # Recebe os dados da requisição
        data = request.get_json()

        # Verifica se o payload é uma lista de vendas ou uma única venda
        if isinstance(data, list):
            # Caso seja uma lista de vendas
            for sale in data:
                product_id = sale['product_id']
                sales_ts = sale['sales_ts']
                quantity = sale['quantity']

                # Conectar ao banco
                raw_engine, _ = get_engines()

                query = text("""
                    INSERT INTO sales (product_id, sales_ts, quantity)
                    VALUES (:product_id, :sales_ts, :quantity)
                """)

                with raw_engine.connect() as connection:
                    transaction = connection.begin()
                    try:
                        connection.execute(query, {
                            "product_id": product_id,
                            "sales_ts": sales_ts,
                            "quantity": quantity
                        })
                        transaction.commit()
                    except Exception as e:
                        transaction.rollback()
                        raise e

            return jsonify({"message": "Sales added successfully"}), 201

        else:
            # Caso seja uma única venda
            product_id = data['product_id']
            sales_ts = data['sales_ts']
            quantity = data['quantity']

            # Conectar ao banco
            raw_engine, _ = get_engines()

            query = text("""
                INSERT INTO sales (product_id, sales_ts, quantity)
                VALUES (:product_id, :sales_ts, :quantity)
            """)

            with raw_engine.connect() as connection:
                transaction = connection.begin()
                try:
                    connection.execute(query, {
                        "product_id": product_id,
                        "sales_ts": sales_ts,
                        "quantity": quantity
                    })
                    transaction.commit()
                except Exception as e:
                    transaction.rollback()
                    raise e

            return jsonify({"message": "Sale added successfully"}), 201

    except Exception as e:
        logging.error(f"Error adding sale: {str(e)}")
        return jsonify({"error": "Error adding sale"}), 500



if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0')
