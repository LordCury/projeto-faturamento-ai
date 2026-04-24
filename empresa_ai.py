import os
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Carrega variáveis do .env
load_dotenv(override=True)


def get_engine():
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "5432")
    db = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")

    if not all([host, port, db, user, password]):
        raise ValueError(
            "Configuração do banco incompleta no .env. "
            "Verifique DB_HOST, DB_PORT, DB_NAME, DB_USER e DB_PASS."
        )

    user_enc = quote_plus(user)
    password_enc = quote_plus(password)

    conn_str = f"postgresql+psycopg2://{user_enc}:{password_enc}@{host}:{port}/{db}"
    return create_engine(conn_str, pool_pre_ping=True)


def executar_consulta(query: str) -> pd.DataFrame:
    engine = get_engine()
    return pd.read_sql(query, engine)


def executar_sql(sql: str):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(sql))


def listar_tabelas_schema(schema: str = None):
    engine = get_engine()
    schema = schema or os.getenv("APP_SCHEMA", "empresa_ai")

    query = f"""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = '{schema}'
    ORDER BY table_name
    """

    return pd.read_sql(query, engine)


def listar_logs_etl():
    schema = os.getenv("APP_SCHEMA", "empresa_ai")
    query = f"""
    SELECT *
    FROM {schema}.etl_execucoes_log
    ORDER BY id DESC
    LIMIT 100
    """
    return executar_consulta(query)