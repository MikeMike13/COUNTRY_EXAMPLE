import psycopg2
import pandas as pd
import sys
import os
from dotenv import load_dotenv

load_dotenv()

# Глобальная переменная для хранения одного соединения
_connection = None

def get_connection():
    """Создает и возвращает единое соединение с БД"""
    global _connection
    if _connection is None or _connection.closed != 0:
        try:
            _connection = psycopg2.connect(
                dbname=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT"),
            )
            # Вывод в стиле fprintf для контроля процесса в консоли/ноутбуке
            # sys.stdout.write("--- INFO: Connected to PostgreSQL successfully ---\n")
        except Exception as e:
            sys.stdout.write("--- ERROR: Unable to connect to database: %s ---\n" % e)
            return None
    return _connection

def sovdb_read(ticker, date):
    """Загружает данные по тикеру и дате"""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame() # Возвращаем пустой DF при ошибке

    query = "SELECT * FROM sovdb_schema.\""+ticker+"\" WHERE \"""Date\""">='"+date.strftime('%Y-%m-%d')+"'"    
    
    try:
        # Используем pandas для удобства создания DataFrame напрямую из SQL
        df = pd.read_sql_query(query, conn, params=(ticker, date))
        df = pd.DataFrame(df).set_index('Date')
        df.index = pd.to_datetime(df.index)    
        df = df.sort_index()

        # Логирование загрузки
        # sys.stdout.write("--- DATA: Loaded %d rows for [%s] on %s ---\n" % (len(df), ticker, date))
        return df
    except Exception as e:
        sys.stdout.write("--- ERROR: Query failed: %s ---\n" % e)
        return pd.DataFrame()