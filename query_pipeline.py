import os
import re
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(override=True)


class QueryAdaptationPipeline:
    def __init__(self):
        self.base_dir = Path(__file__).resolve().parent
        self.project_root = self.base_dir

        catalog_env = os.getenv("QUERY_CATALOG_PATH", "data/BANCO_QUERY.csv")
        output_env = os.getenv("CORRECTED_QUERY_DIR", "data/corrigidas")

        self.catalog_path = (self.project_root / catalog_env).resolve()
        self.output_dir = (self.project_root / output_env).resolve()
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.logs_dir = (self.project_root / "logs").resolve()
        self.logs_dir.mkdir(parents=True, exist_ok=True)

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
            raise ValueError("Variáveis de banco incompletas no .env.")

        conn_str = (
            f"postgresql+psycopg2://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{db}"
        )
        return create_engine(conn_str, pool_pre_ping=True)

    # =========================
    # LEITURA DO CATÁLOGO
    # =========================
    def ler_catalogo(self) -> pd.DataFrame:
        encodings = ["utf-8-sig", "utf-8", "latin1", "cp1252"]
        separadores = [";", ",", "\t"]

        if not os.path.exists(self.catalog_path):
            raise FileNotFoundError(f"Catálogo não encontrado: {self.catalog_path}")

        df = None
        ultimo_erro = None

        for encoding in encodings:
            for sep in separadores:
                try:
                    tmp = pd.read_csv(self.catalog_path, encoding=encoding, sep=sep)
                    if tmp.shape[1] >= 2:
                        df = tmp.iloc[:, :2].copy()
                        break
                except Exception as e:
                    ultimo_erro = e
            if df is not None:
                break

        if df is None:
            raise ValueError(f"Não foi possível ler o catálogo. Último erro: {ultimo_erro}")

        df.columns = ["canal", "query_original"]
        df["canal"] = df["canal"].astype(str).str.strip()
        df["query_original"] = df["query_original"].astype(str).str.strip()
        df = df[(df["canal"] != "") & (df["query_original"] != "")]
        return df

    # =========================
    # NORMALIZAÇÃO
    # =========================
    def normalizar_nome_arquivo(self, canal: str) -> str:
        nome = canal.strip().lower()
        nome = re.sub(r"[^a-z0-9_]+", "_", nome)
        nome = re.sub(r"_+", "_", nome).strip("_")
        return f"{nome}.sql"

    # =========================
    # CORREÇÕES
    # =========================
    def corrigir_placeholders(self, query: str) -> str:
        substituicoes = {
            "{Start}": "'2024-01-01 00:00:00'",
            "{End}": "'2026-12-31 23:59:59'",
            "{Start_1}": "'2024-01-01 00:00:00'",
            "{End_1}": "'2026-12-31 23:59:59'",
            "{Start_M}": "'2024-01-01 00:00:00'",
        }

        for antigo, novo in substituicoes.items():
            query = query.replace(antigo, novo)

        return query

    def corrigir_aspas_duplicadas(self, query: str) -> str:
        return query.replace('""', '"')

    def corrigir_encoding_basico(self, query: str) -> str:
        correcoes = {
            "LigaÃ§Ã£o": "Ligação",
            "conteÃºdo": "conteúdo",
            "NÃ£o": "Não",
            "invÃ¡lido": "inválido",
            "NavegaÃ§Ã£o": "Navegação",
            "DuraÃ§Ã£o": "Duração",
            "AtenÃ§Ã£o": "Atenção",
            "BenefÃ­cio": "Benefício",
            "ConversaÃ§Ã£o": "Conversação",
            "OperaÃ§Ã£o": "Operação",
            "ServiÃ§o": "Serviço",
        }
        for errado, certo in correcoes.items():
            query = query.replace(errado, certo)
        return query

    def limpar_query(self, query: str) -> str:
        return query.strip().rstrip(";").strip()

    def adaptar_query(self, query: str) -> str:
        query = self.corrigir_placeholders(query)
        query = self.corrigir_aspas_duplicadas(query)
        query = self.corrigir_encoding_basico(query)
        query = self.limpar_query(query)
        return query

    # =========================
    # ARQUIVOS
    # =========================
    def salvar_query_corrigida(self, canal: str, query_corrigida: str) -> Path:
        caminho = self.output_dir / self.normalizar_nome_arquivo(canal)
        caminho.write_text(query_corrigida, encoding="utf-8")
        return caminho

    # =========================
    # TESTE
    # =========================
    def testar_query(self, query: str, limite: int = 1) -> tuple[bool, str]:
        query_limpa = self.limpar_query(query)
        query_teste = f"SELECT * FROM ({query_limpa}) q LIMIT {limite}"

        try:
            with self.engine.begin() as conn:
                conn.execute(text(query_teste))
            return True, "OK"
        except Exception as e:
            return False, str(e)

    # =========================
    # PROCESSAMENTO
    # =========================
    def processar_catalogo(self) -> pd.DataFrame:
        df = self.ler_catalogo()
        resultados = []

        for _, row in df.iterrows():
            canal = row["canal"]
            query_original = row["query_original"]

            try:
                query_corrigida = self.adaptar_query(query_original)
                caminho_sql = self.salvar_query_corrigida(canal, query_corrigida)

                ok, retorno_teste = self.testar_query(query_corrigida)

                resultados.append({
                    "canal": canal,
                    "arquivo_sql": str(caminho_sql),
                    "status_teste": "SUCESSO" if ok else "ERRO_EXECUCAO",
                    "tamanho_query_original": len(query_original),
                    "tamanho_query_corrigida": len(query_corrigida),
                    "detalhe": retorno_teste,
                })

                print(f"[{'OK' if ok else 'ERRO'}] {canal}")

            except Exception as e:
                resultados.append({
                    "canal": canal,
                    "arquivo_sql": None,
                    "status_teste": "ERRO_ADAPTACAO",
                    "tamanho_query_original": len(query_original) if query_original else 0,
                    "tamanho_query_corrigida": None,
                    "detalhe": str(e),
                })

                print(f"[ERRO_ADAPTACAO] {canal}")

        df_result = pd.DataFrame(resultados)
        df_result.to_csv(
            self.logs_dir / "resultado_correcao_queries.csv",
            index=False,
            encoding="utf-8-sig"
        )
        return df_result


if __name__ == "__main__":
    pipeline = QueryAdaptationPipeline()
    resultado = pipeline.processar_catalogo()
    print(resultado.head())