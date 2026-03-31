from crewai.tools import tool
import subprocess
import os

@tool("ler_arquivo_csv")
def ler_arquivo_csv(caminho_arquivo: str, **kwargs) -> str:
    """
    Use esta ferramenta APENAS para ler a estrutura (colunas e tipos) de um arquivo CSV.
    """
    try:
        # Lê o arquivo ignorando erros de caracteres especiais
        with open(caminho_arquivo, 'r', encoding='utf-8', errors='ignore') as f:
            linhas = f.readlines()
            # Retorna APENAS as primeiras 60 linhas para não estourar a memória da IA
            return "".join(linhas[:60])
    except Exception as e:
        return f"Erro ao ler o arquivo: {e}"
    

@tool("git_manager_tool")
def git_manager_tool(commit_message: str):
    """
    Útil para salvar o progresso do trabalho no GitHub. 
    Executa git add, commit e push de forma segura.
    """
    try:
        # Executa os comandos em sequência
        subprocess.run(["git", "add", "."], check=True)
        subprocess.run(["git", "commit", "-m", commit_message], check=True)
        subprocess.run(["git", "push"], check=True)
        
        return f"Sucesso: Alterações enviadas com a mensagem: '{commit_message}'"
    except subprocess.CalledProcessError as e:
        return f"Erro ao executar comando Git: {e}"
    except Exception as e:
        return f"Erro inesperado: {e}"


@tool("file_validator_tool")
def file_validator_tool(file_path: str):
    """
    Verifica se um arquivo existe e se contém dados antes de iniciar a análise.
    """
    if os.path.exists(file_path):
        size = os.path.getsize(file_path)
        if size > 0:
            return f"Arquivo '{file_path}' validado. Tamanho: {size} bytes."
        return f"Alerta: O arquivo '{file_path}' existe, mas está vazio."
    return f"Erro: O arquivo '{file_path}' não foi encontrado."