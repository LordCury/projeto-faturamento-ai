import os
import re
from pathlib import Path
from urllib.parse import quote_plus
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

from query_version_manager import QueryVersionManager

load_dotenv(override=True)


class QueryAutoFixEngine:
    def __init__(self):
        self.base_dir = Path(__file__).resolve().parent
        self.corrected_dir = self.base_dir / "data" / "corrigidas"
        self.logs_dir = self.base_dir / "logs"
        self.logs_dir.mkdir(parents=True, exist_ok=True)

        self.manager = QueryVersionManager()
        self.engine = self._create_engine()

    def _create_engine(self):
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT", "5432")
        db = os.getenv("DB_NAME")
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASS")

        conn_str = (
            f"postgresql+psycopg2://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{db}"
        )
        return create_engine(conn_str, pool_pre_ping=True)

    def corrigir_to_date(sql):
        return re.sub(
            r"to_date\(\s*to_chara::date\s*,\s*'dd/mm/yyyy'\s*\),\s*'dd/mm/yyyy'\)",
            "to_date(to_char(c.data_hora::date, 'dd/mm/yyyy'), 'dd/mm/yyyy')",
            sql
        )

    def listar_queries(self):
        return sorted(self.corrected_dir.glob("*.sql"))

    def testar_query(self, query: str):
        query = query.strip().rstrip(";")
        sql = f"SELECT * FROM ({query}) q LIMIT 1"

        try:
            with self.engine.begin() as conn:
                conn.execute(text(sql))
            return True, "OK"
        except Exception as e:
            return False, str(e)

    def corrigir_where(sql):
        return sql.replace("WHERE q.data", "WHERE q.dia")

    def corrigir_parenteses(sql):
        return sql.replace("), 'dd/mm/yyyy')", ", 'dd/mm/yyyy')")

    def aplicar_correcoes_basicas(self, query: str) -> tuple[str, list[str]]:
        alteracoes = []

        original = query

        substituicoes = {
            # Typos comuns
            "tatus_whatsapp": "status_whatsapp",
            "to_chara": "to_char",
            "inicio_telamp": "inicio_telegram_stamp",
            "Recebido_Elenico": "Recebido_Eletronico",
            "Humano_eetronico": "Humano_eletronico",
            "recebidono": "Recebido_humano",

            # Correção de filtro final
            "q.data": "q.dia",

            # Correção de erro lógico Power Query
            "then else 0": "then 1 else 0",
        }

        for errado, certo in substituicoes.items():
            if errado in query:
                query = query.replace(errado, certo)
                alteracoes.append(f"Substituído `{errado}` por `{certo}`")

        # Corrige caso específico quebrado:
        query = query.replace(
            "to_date(c.data_hora::date , 'dd/mm/yyyy'), 'dd/mm/yyyy')  as dia",
            "c.data_hora::date as dia"
        )

        query = query.replace(
            "to_date(to_char(c.data_hora::date , 'dd/mm/yyyy'), 'dd/mm/yyyy')  as dia",
            "c.data_hora::date as dia"
        )

        # Remove ponto e vírgula final
        query = query.strip().rstrip(";").strip()

        if query != original and not alteracoes:
            alteracoes.append("Correções estruturais aplicadas")

        return query, alteracoes

    def processar(self, limite_queries: int = 5):
        resultados = []

        arquivos = self.listar_queries()[:limite_queries]

        for arquivo in arquivos:
            canal = arquivo.stem
            query_original = arquivo.read_text(encoding="utf-8", errors="ignore")

            ok_antes, erro_antes = self.testar_query(query_original)

            if ok_antes:
                resultados.append({
                    "canal": canal,
                    "arquivo": str(arquivo),
                    "status": "JA_FUNCIONAVA",
                    "erro_antes": "",
                    "alteracoes": "",
                    "erro_depois": ""
                })
                print(f"[OK] {canal} já funcionava")
                continue

            query_corrigida, alteracoes = self.aplicar_correcoes_basicas(query_original)

            if not alteracoes:
                resultados.append({
                    "canal": canal,
                    "arquivo": str(arquivo),
                    "status": "SEM_CORRECAO_AUTOMATICA",
                    "erro_antes": erro_antes[:1000],
                    "alteracoes": "",
                    "erro_depois": ""
                })
                print(f"[SEM_AUTO_FIX] {canal}")
                continue

            ok_depois, erro_depois = self.testar_query(query_corrigida)

            if ok_depois:
                self.manager.salvar_query_corrigida(
                    canal=canal,
                    query_corrigida=query_corrigida,
                    origem_correcao="auto_fix_v3",
                    observacao="; ".join(alteracoes),
                    erro_original=erro_antes,
                )

                status = "CORRIGIDA_COM_SUCESSO"
                print(f"[CORRIGIDA] {canal}")

            else:
                status = "CORRECAO_GERADA_MAS_AINDA_COM_ERRO"
                print(f"[AINDA_ERRO] {canal}")

            resultados.append({
                "canal": canal,
                "arquivo": str(arquivo),
                "status": status,
                "erro_antes": erro_antes[:1000],
                "alteracoes": "; ".join(alteracoes),
                "erro_depois": erro_depois[:1000],
            })

        df = pd.DataFrame(resultados)
        caminho_log = self.logs_dir / "auto_fix_v3_resultado.csv"
        df.to_csv(caminho_log, index=False, encoding="utf-8-sig")

        return df


if __name__ == "__main__":
    engine = QueryAutoFixEngine()
    resultado = engine.processar(limite_queries=5)
    print(resultado)