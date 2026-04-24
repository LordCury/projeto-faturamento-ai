import subprocess
from pathlib import Path

import pandas as pd
from crewai.tools import tool


BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR


def _resolver_caminho(caminho: str) -> Path:
    path = Path(caminho)
    if path.is_absolute():
        return path
    return (PROJECT_ROOT / path).resolve()


def _ler_csv_generico(file_path: str, nrows=None):
    caminho = _resolver_caminho(file_path)

    if not caminho.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho}")

    if caminho.stat().st_size == 0:
        raise ValueError(f"Arquivo existe, mas está vazio: {caminho}")

    encodings = ["utf-8-sig", "utf-8", "latin1", "cp1252"]
    separadores = [";", ",", "\t"]

    ultimo_erro = None

    for encoding in encodings:
        for sep in separadores:
            try:
                df = pd.read_csv(caminho, encoding=encoding, sep=sep, nrows=nrows)
                if df.shape[1] >= 2:
                    return df, encoding, sep, caminho
            except Exception as e:
                ultimo_erro = e

    raise ValueError(f"Não foi possível ler o arquivo. Último erro: {ultimo_erro}")


@tool("ler_arquivo_csv")
def ler_arquivo_csv(file_path: str, **kwargs) -> str:
    """
    Lê um arquivo CSV, identifica encoding/separador e retorna colunas e amostra inicial.
    """
    try:
        df, encoding, sep, caminho = _ler_csv_generico(file_path, nrows=10)

        return (
            f"Arquivo: {caminho}\n"
            f"Encoding usado: {encoding}\n"
            f"Separador usado: '{sep}'\n"
            f"Quantidade de colunas: {len(df.columns)}\n"
            f"Colunas: {df.columns.tolist()}\n"
            f"Amostra: {df.head(10).to_dict(orient='records')}"
        )
    except Exception as e:
        return f"Erro ao ler o arquivo: {e}"


@tool("ler_catalogo_queries_tool")
def ler_catalogo_queries_tool(file_path: str, max_registros: int = 10, **kwargs) -> str:
    """
    Lê o catálogo de queries e retorna um resumo dos primeiros registros.
    """
    try:
        df, encoding, sep, caminho = _ler_csv_generico(file_path)

        df = df.iloc[:, :2].copy()
        df.columns = ["canal", "query_sql"]
        df["canal"] = df["canal"].astype(str).fillna("").str.strip()
        df["query_sql"] = df["query_sql"].astype(str).fillna("").str.strip()

        registros = []
        total = min(len(df), max_registros)

        for idx, row in df.head(max_registros).iterrows():
            query_limpa = " ".join(row["query_sql"].split())
            registros.append(
                f"Registro {idx + 1}:\n"
                f"Canal: {row['canal']}\n"
                f"Query resumo: {query_limpa[:800]}\n"
            )

        return (
            f"Arquivo: {caminho}\n"
            f"Encoding usado: {encoding}\n"
            f"Separador usado: '{sep}'\n"
            f"Total de registros: {len(df)}\n"
            f"Resumo dos primeiros {total} registros:\n\n"
            + "\n".join(registros)
        )
    except Exception as e:
        return f"Erro ao ler o catálogo de queries: {e}"


@tool("extrair_canais_catalogo_tool")
def extrair_canais_catalogo_tool(file_path: str, **kwargs) -> str:
    """
    Extrai a lista de canais existentes no catálogo de queries.
    """
    try:
        df, _, _, caminho = _ler_csv_generico(file_path)

        canais = (
            df.iloc[:, 0]
            .astype(str)
            .fillna("")
            .str.strip()
            .tolist()
        )

        canais_unicos = sorted(set([c for c in canais if c]))

        return (
            f"Arquivo: {caminho}\n"
            f"Total de canais encontrados: {len(canais_unicos)}\n"
            f"Canais: {canais_unicos}"
        )
    except Exception as e:
        return f"Erro ao extrair canais do catálogo: {e}"


@tool("buscar_query_por_canal_tool")
def buscar_query_por_canal_tool(file_path: str, canal: str, **kwargs) -> str:
    """
    Busca e retorna a query completa de um canal específico no catálogo.
    """
    try:
        df, _, _, caminho = _ler_csv_generico(file_path)

        df = df.iloc[:, :2].copy()
        df.columns = ["canal", "query_sql"]
        df["canal"] = df["canal"].astype(str).fillna("").str.strip()

        resultado = df[df["canal"].str.upper() == canal.upper()]

        if resultado.empty:
            canais_disponiveis = sorted(set(df["canal"].tolist()))
            return (
                f"Canal '{canal}' não encontrado no arquivo {caminho}.\n"
                f"Canais disponíveis: {canais_disponiveis[:20]}"
            )

        return (
            f"Arquivo: {caminho}\n"
            f"Canal: {canal}\n"
            f"Query completa:\n{resultado.iloc[0]['query_sql']}"
        )
    except Exception as e:
        return f"Erro ao buscar query por canal: {e}"


@tool("git_manager_tool")
def git_manager_tool(commit_message: str, **kwargs) -> str:
    """
    Executa git add, git commit e git push para versionar alterações do projeto.
    """
    try:
        subprocess.run(["git", "add", "."], check=True)

        commit_result = subprocess.run(
            ["git", "commit", "-m", commit_message],
            capture_output=True,
            text=True
        )

        stdout = (commit_result.stdout or "").strip()
        stderr = (commit_result.stderr or "").strip()
        mensagem = f"{stdout}\n{stderr}".strip().lower()

        if "nothing to commit" in mensagem or "nada para commitar" in mensagem:
            return "Aviso: Não havia alterações novas para commit."

        if commit_result.returncode != 0:
            return f"Erro ao executar git commit: {stdout or stderr}"

        push_result = subprocess.run(
            ["git", "push"],
            check=True,
            capture_output=True,
            text=True
        )

        return (
            f"Sucesso: Alterações enviadas com a mensagem: '{commit_message}'.\n"
            f"{push_result.stdout.strip()}"
        )
    except subprocess.CalledProcessError as e:
        return f"Erro ao executar comando Git: {e}"
    except Exception as e:
        return f"Erro inesperado no Git: {e}"


@tool("file_validator_tool")
def file_validator_tool(file_path: str, **kwargs) -> str:
    """
    Valida se um arquivo existe e se possui conteúdo.
    """
    try:
        caminho = _resolver_caminho(file_path)

        if caminho.exists():
            size = caminho.stat().st_size
            if size > 0:
                return f"Arquivo '{caminho}' validado. Tamanho: {size} bytes."
            return f"Alerta: O arquivo '{caminho}' existe, mas está vazio."

        return f"Erro: O arquivo '{caminho}' não foi encontrado."
    except Exception as e:
        return f"Erro inesperado ao validar o arquivo: {e}"


@tool("listar_queries_corrigidas_tool")
def listar_queries_corrigidas_tool(directory_path: str = "data/corrigidas", **kwargs) -> str:
    """
    Lista arquivos SQL corrigidos existentes no diretório de queries corrigidas.
    """
    try:
        caminho = _resolver_caminho(directory_path)

        if not caminho.exists():
            return f"Diretório não encontrado: {caminho}"

        arquivos = [
            f.name for f in caminho.iterdir()
            if f.is_file() and f.suffix.lower() == ".sql"
        ]

        return f"Total de queries corrigidas: {len(arquivos)}\nArquivos: {arquivos}"
    except Exception as e:
        return f"Erro ao listar queries corrigidas: {e}"


@tool("ler_query_corrigida_tool")
def ler_query_corrigida_tool(file_path: str, **kwargs) -> str:
    """
    Lê o conteúdo completo de uma query SQL corrigida.
    """
    try:
        caminho = _resolver_caminho(file_path)

        if not caminho.exists():
            return f"Arquivo não encontrado: {caminho}"

        return caminho.read_text(encoding="utf-8", errors="ignore")
    except Exception as e:
        return f"Erro ao ler query corrigida: {e}"