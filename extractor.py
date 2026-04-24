import os
import re
from datetime import datetime
from typing import Optional, Tuple
from urllib.parse import quote_plus
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


# Carrega variáveis do .env
load_dotenv(override=True)


class ETLExtractor:
    def __init__(self):
        self.base_dir = Path(__file__).resolve().parent
        self.project_root = self.base_dir

        self.app_schema = os.getenv("APP_SCHEMA", "empresa_ai")

        catalog_env = os.getenv("QUERY_CATALOG_PATH", "data/BANCO_QUERY.csv")
        corrected_env = os.getenv("CORRECTED_QUERY_DIR", "data/corrigidas")

        self.catalog_path = (self.project_root / catalog_env).resolve()
        self.corrected_query_dir = (self.project_root / corrected_env).resolve()

        self.engine = self._create_engine()

    # =========================
    # CONEXÃO
    # =========================
    def _create_engine(self):
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT", "5432")
        db = os.getenv("DB_NAME")
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASS")

        if not all([host, port, db, user, password]):
            raise ValueError(
                "Configuração DB incompleta no .env. "
                "Esperado: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS."
            )

        user_enc = quote_plus(user)
        password_enc = quote_plus(password)

        conn_str = f"postgresql+psycopg2://{user_enc}:{password_enc}@{host}:{port}/{db}"
        return create_engine(conn_str, pool_pre_ping=True)

    # =========================
    # VALIDAÇÕES
    # =========================
    def _schema_existe(self) -> bool:
        query = text("""
            SELECT 1
            FROM information_schema.schemata
            WHERE schema_name = :schema_name
        """)
        with self.engine.begin() as conn:
            result = conn.execute(query, {"schema_name": self.app_schema}).fetchone()
            return result is not None

    def _garantir_tabela_log(self):
        script = f"""
        CREATE TABLE IF NOT EXISTS {self.app_schema}.etl_execucoes_log (
            id BIGSERIAL PRIMARY KEY,
            canal VARCHAR(255) NOT NULL,
            tabela_destino VARCHAR(255) NOT NULL,
            inicio_execucao TIMESTAMP NOT NULL,
            fim_execucao TIMESTAMP NULL,
            status VARCHAR(50) NOT NULL,
            linhas_extraidas BIGINT NULL,
            mensagem TEXT NULL,
            duracao_segundos NUMERIC(18,2) NULL,
            modo_carga VARCHAR(50) NULL,
            fonte_query TEXT NULL
        );
        """
        with self.engine.begin() as conn:
            conn.execute(text(script))

    # =========================
    # UTILITÁRIOS
    # =========================
    def _normalizar_nome_tabela(self, canal: str) -> str:
        nome = canal.strip().lower()
        nome = re.sub(r"[^a-z0-9_]+", "_", nome)
        nome = re.sub(r"_+", "_", nome).strip("_")
        return f"stg_{nome}"

    def _normalizar_nome_arquivo_query(self, canal: str) -> str:
        nome = canal.strip().lower()
        nome = re.sub(r"[^a-z0-9_]+", "_", nome)
        nome = re.sub(r"_+", "_", nome).strip("_")
        return f"{nome}.sql"

    # =========================
    # LOG
    # =========================
    def _registrar_log(
        self,
        canal: str,
        tabela_destino: str,
        inicio_execucao: datetime,
        fim_execucao: Optional[datetime],
        status: str,
        linhas_extraidas: Optional[int] = None,
        mensagem: Optional[str] = None,
        modo_carga: Optional[str] = None,
        fonte_query: Optional[str] = None,
    ):
        duracao = None
        if fim_execucao:
            duracao = round((fim_execucao - inicio_execucao).total_seconds(), 2)

        sql = text(f"""
            INSERT INTO {self.app_schema}.etl_execucoes_log (
                canal,
                tabela_destino,
                inicio_execucao,
                fim_execucao,
                status,
                linhas_extraidas,
                mensagem,
                duracao_segundos,
                modo_carga,
                fonte_query
            )
            VALUES (
                :canal,
                :tabela_destino,
                :inicio_execucao,
                :fim_execucao,
                :status,
                :linhas_extraidas,
                :mensagem,
                :duracao_segundos,
                :modo_carga,
                :fonte_query
            )
        """)

        with self.engine.begin() as conn:
            conn.execute(sql, {
                "canal": canal,
                "tabela_destino": tabela_destino,
                "inicio_execucao": inicio_execucao,
                "fim_execucao": fim_execucao,
                "status": status,
                "linhas_extraidas": linhas_extraidas,
                "mensagem": mensagem,
                "duracao_segundos": duracao,
                "modo_carga": modo_carga,
                "fonte_query": fonte_query,
            })

    # =========================
    # LEITURA CATÁLOGO
    # =========================
    def _ler_catalogo_queries(self) -> pd.DataFrame:
        if not os.path.exists(self.catalog_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {self.catalog_path}")

        encodings = ["utf-8-sig", "utf-8", "latin1", "cp1252"]
        separadores = [";", ",", "\t"]

        for encoding in encodings:
            for sep in separadores:
                try:
                    df = pd.read_csv(self.catalog_path, encoding=encoding, sep=sep)
                    if df.shape[1] >= 2:
                        df = df.iloc[:, :2].copy()
                        df.columns = ["canal", "query_sql"]
                        return df.dropna()
                except Exception:
                    continue

        raise ValueError("Falha ao ler catálogo de queries.")

    # =========================
    # QUERIES
    # =========================
    def _carregar_query_corrigida(self, canal: str) -> Tuple[Optional[str], Optional[str]]:
        nome = self._normalizar_nome_arquivo_query(canal)
        caminho = self.corrected_query_dir / nome

        if caminho.exists():
            return caminho.read_text(encoding="utf-8", errors="ignore"), str(caminho)

        return None, None

    def _resolver_query(self, canal: str, query_original: str) -> Tuple[str, str]:
        query_corrigida, caminho = self._carregar_query_corrigida(canal)

        if query_corrigida:
            return query_corrigida, f"corrigida:{caminho}"

        return query_original, "catalogo_original"

    # =========================
    # INCREMENTAL
    # =========================
    def _extrair_campo_data(self, query_sql: str) -> Optional[str]:
        candidatos = ["data", "dt", "data_atendimento", "created_at"]

        query_lower = query_sql.lower()
        for campo in candidatos:
            if campo in query_lower:
                return campo
        return None

    def _montar_incremental(self, query: str, campo: str, dias: int) -> str:
        return f"""
        SELECT *
        FROM ({query}) q
        WHERE q.{campo} >= CURRENT_DATE - INTERVAL '{dias} day'
        """

    # =========================
    # EXECUÇÃO
    # =========================
    def executar_canal(self, canal: str, query_sql: str, modo_carga="replace", incremental=False, dias=1):
        tabela = self._normalizar_nome_tabela(canal)
        inicio = datetime.now()

        try:
            query_base, fonte = self._resolver_query(canal, query_sql)

            if incremental:
                campo = self._extrair_campo_data(query_base)
                if campo:
                    query_base = self._montar_incremental(query_base, campo, dias)

            df = pd.read_sql(text(query_base), self.engine)

            if df.empty:
                self._registrar_log(canal, tabela, inicio, datetime.now(), "SEM_DADOS", 0, "Sem retorno", modo_carga, fonte)
                return

            df.to_sql(
                name=tabela,
                con=self.engine,
                schema=self.app_schema,
                if_exists="replace" if modo_carga == "replace" else "append",
                index=False,
                method="multi",
                chunksize=1000,
            )

            self._registrar_log(canal, tabela, inicio, datetime.now(), "SUCESSO", len(df), None, modo_carga, fonte)

        except Exception as e:
            self._registrar_log(canal, tabela, inicio, datetime.now(), "ERRO", None, str(e), modo_carga, "erro")
            print(f"[ERRO] {canal}: {e}")

    def executar_todos(self, modo_carga="replace", incremental=False, dias=1):
        if not self._schema_existe():
            raise ValueError(f"Schema {self.app_schema} não existe.")

        self._garantir_tabela_log()

        df = self._ler_catalogo_queries()

        for _, row in df.iterrows():
            self.executar_canal(
                canal=row["canal"],
                query_sql=row["query_sql"],
                modo_carga=modo_carga,
                incremental=incremental,
                dias=dias,
            )


if __name__ == "__main__":
    ETLExtractor().executar_todos(
        modo_carga="truncate_append",
        incremental=True,
        dias=1
    )