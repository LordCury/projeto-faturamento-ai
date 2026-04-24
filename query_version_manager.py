import os
import re
import shutil
from pathlib import Path
from datetime import datetime
import pandas as pd


class QueryVersionManager:
    def __init__(self):
        self.base_dir = Path(__file__).resolve().parent
        self.corrected_dir = self.base_dir / "data" / "corrigidas"
        self.version_dir = self.base_dir / "data" / "versoes_queries"
        self.rollback_dir = self.base_dir / "data" / "rollback_queries"
        self.logs_dir = self.base_dir / "logs"

        for folder in [
            self.corrected_dir,
            self.version_dir,
            self.rollback_dir,
            self.logs_dir,
        ]:
            folder.mkdir(parents=True, exist_ok=True)

        self.audit_file = self.logs_dir / "auditoria_queries.csv"

    def normalizar_nome(self, canal: str) -> str:
        nome = canal.strip().lower()
        nome = re.sub(r"[^a-z0-9_]+", "_", nome)
        nome = re.sub(r"_+", "_", nome).strip("_")
        return nome

    def caminho_query_corrigida(self, canal: str) -> Path:
        return self.corrected_dir / f"{self.normalizar_nome(canal)}.sql"

    def criar_backup_versao(self, canal: str) -> str:
        origem = self.caminho_query_corrigida(canal)

        if not origem.exists():
            return ""

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        destino = self.version_dir / f"{self.normalizar_nome(canal)}__{timestamp}.sql"

        shutil.copy2(origem, destino)
        return str(destino)

    def salvar_query_corrigida(
        self,
        canal: str,
        query_corrigida: str,
        origem_correcao: str,
        observacao: str = "",
        erro_original: str = "",
    ) -> Path:
        backup = self.criar_backup_versao(canal)
        destino = self.caminho_query_corrigida(canal)

        destino.write_text(query_corrigida.strip(), encoding="utf-8")

        self.registrar_auditoria(
            canal=canal,
            arquivo_corrigido=str(destino),
            backup_anterior=backup,
            origem_correcao=origem_correcao,
            observacao=observacao,
            erro_original=erro_original,
        )

        return destino

    def registrar_auditoria(
        self,
        canal: str,
        arquivo_corrigido: str,
        backup_anterior: str,
        origem_correcao: str,
        observacao: str,
        erro_original: str,
    ):
        registro = {
            "data_hora": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "canal": canal,
            "arquivo_corrigido": arquivo_corrigido,
            "backup_anterior": backup_anterior,
            "origem_correcao": origem_correcao,
            "observacao": observacao,
            "erro_original": erro_original[:1000] if erro_original else "",
        }

        df_novo = pd.DataFrame([registro])

        if self.audit_file.exists():
            df_antigo = pd.read_csv(self.audit_file, encoding="utf-8-sig")
            df_final = pd.concat([df_antigo, df_novo], ignore_index=True)
        else:
            df_final = df_novo

        df_final.to_csv(self.audit_file, index=False, encoding="utf-8-sig")

    def listar_versoes(self, canal: str):
        prefixo = self.normalizar_nome(canal)
        return sorted(self.version_dir.glob(f"{prefixo}__*.sql"))

    def rollback_ultima_versao(self, canal: str) -> str:
        versoes = self.listar_versoes(canal)

        if not versoes:
            return f"Nenhuma versão anterior encontrada para {canal}."

        ultima = versoes[-1]
        destino = self.caminho_query_corrigida(canal)

        shutil.copy2(ultima, destino)

        return f"Rollback realizado para {canal} usando {ultima}."