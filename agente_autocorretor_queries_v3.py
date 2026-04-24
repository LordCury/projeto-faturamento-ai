import os
from dotenv import load_dotenv
from crewai import Agent, Task, Crew, Process, LLM

from toolsformyagents import (
    listar_queries_corrigidas_tool,
    ler_query_corrigida_tool,
    file_validator_tool,
    git_manager_tool,
)

load_dotenv(override=True)

llm_local = LLM(
    model=os.getenv("MODEL", "ollama/llama3.2"),
    base_url=os.getenv("BASE_URL", "http://localhost:11434")
)

agente_autocorretor = Agent(
    role="Autocorretor SQL PostgreSQL V3",
    goal=(
        "Analisar o resultado do auto-fix V3, validar riscos e documentar quais "
        "correções automáticas foram aplicadas ou ainda precisam de revisão humana."
    ),
    backstory=(
        "Especialista em SQL PostgreSQL, ETL, migração de Power Query e auditoria "
        "de correções automáticas. Atua com cautela para preservar regra de negócio."
    ),
    llm=llm_local,
    tools=[
        listar_queries_corrigidas_tool,
        ler_query_corrigida_tool,
        file_validator_tool,
    ],
    verbose=True
)

agente_git = Agent(
    role="Gestor de Versionamento Seguro",
    goal="Versionar somente artefatos gerados e relatórios da V3.",
    backstory="Especialista em Git, rastreabilidade e auditoria técnica.",
    llm=llm_local,
    tools=[file_validator_tool, git_manager_tool],
    verbose=True
)

tarefa_analise_v3 = Task(
    description="""
    Analise o arquivo logs/auto_fix_v3_resultado.csv e as queries em data/corrigidas.

    Objetivos:
    1. Identificar quais queries foram corrigidas automaticamente.
    2. Identificar quais queries ainda ficaram com erro.
    3. Classificar o risco de cada correção:
       - baixo
       - médio
       - alto
    4. Explicar se a correção preserva a regra de negócio.
    5. Listar quais queries precisam de revisão humana.

    Não sobrescreva arquivos.
    Não invente tabelas ou colunas.
    Gere apenas documentação.
    """,
    expected_output="Relatório de auditoria da autocorreção V3.",
    agent=agente_autocorretor,
    output_file="docs/auditoria_autofix_v3.md"
)

tarefa_git_v3 = Task(
    description="""
    Valide obrigatoriamente os arquivos abaixo usando file_validator_tool.
    Use exatamente estes parâmetros:

    1. file_validator_tool(file_path="logs/auto_fix_v3_resultado.csv")
    2. file_validator_tool(file_path="docs/auditoria_autofix_v3.md")

    Se os dois arquivos existirem e tiverem conteúdo, execute git_manager_tool com:
    commit_message="Auto fix v3 para queries SQL com auditoria"
    """,
    expected_output="Status do versionamento Git.",
    agent=agente_git,
    context=[tarefa_analise_v3]
)

crew_v3 = Crew(
    agents=[agente_autocorretor, agente_git],
    tasks=[tarefa_analise_v3, tarefa_git_v3],
    process=Process.sequential,
    verbose=True
)

if __name__ == "__main__":
    print("### Iniciando Auditoria do Auto Fix V3 ###")
    resultado = crew_v3.kickoff()
    print(resultado)