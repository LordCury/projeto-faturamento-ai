import os
from pathlib import Path
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

agente_diagnostico_sql = Agent(
    role="Especialista em Diagnóstico SQL PostgreSQL",
    goal=(
        "Analisar erros de execução SQL, identificar a causa raiz e classificar "
        "o tipo de problema sem alterar regra de negócio."
    ),
    backstory=(
        "Especialista em PostgreSQL, migração de Power Query para SQL puro, "
        "CTEs, funções de data, tratamento de encoding e pipelines ETL."
    ),
    llm=llm_local,
    tools=[
        listar_queries_corrigidas_tool,
        ler_query_corrigida_tool,
        file_validator_tool,
    ],
    verbose=True
)

agente_refatorador_sql = Agent(
    role="Refatorador SQL PostgreSQL",
    goal=(
        "Propor correções seguras para queries SQL quebradas, preservando "
        "a regra de negócio original."
    ),
    backstory=(
        "Especialista em refatoração SQL, correção de aliases, funções PostgreSQL, "
        "placeholders, erros de sintaxe e compatibilidade com SQLAlchemy."
    ),
    llm=llm_local,
    tools=[
        listar_queries_corrigidas_tool,
        ler_query_corrigida_tool,
        file_validator_tool,
    ],
    verbose=True
)

agente_versionamento = Agent(
    role="Gestor de Versionamento Git",
    goal="Validar artefatos, registrar progresso e versionar alterações relevantes.",
    backstory="Especialista em Git, rastreabilidade, auditoria e versionamento seguro.",
    llm=llm_local,
    tools=[
        file_validator_tool,
        git_manager_tool,
    ],
    verbose=True
)

tarefa_diagnostico = Task(
    description="""
    Analise as queries existentes em data/corrigidas e o arquivo logs/resultado_correcao_queries.csv.

    Objetivos:
    1. Listar as queries corrigidas disponíveis.
    2. Identificar quais queries ainda possuem erro.
    3. Classificar os erros encontrados:
       - erro de sintaxe
       - coluna inexistente
       - alias incorreto
       - erro de encoding
       - placeholder não tratado
       - função PostgreSQL incorreta
       - problema causado por migração Power Query
    4. Gerar diagnóstico técnico por canal.

    Foque em preservar a regra de negócio.
    Não invente tabelas, colunas ou métricas.
    """,
    expected_output="Diagnóstico técnico das queries com erro.",
    agent=agente_diagnostico_sql,
    output_file="docs/diagnostico_queries_v2.md"
)

tarefa_refatoracao = Task(
    description="""
    Com base no diagnóstico gerado, produza recomendações de correção para as queries com erro.

    Para cada query problemática, documente:
    1. Nome do arquivo.
    2. Erro encontrado.
    3. Trecho provável com problema.
    4. Correção sugerida.
    5. Risco da alteração.
    6. Se a correção pode ser aplicada automaticamente ou precisa revisão humana.

    Importante:
    - Não sobrescreva os arquivos .sql diretamente.
    - Gere apenas relatório técnico.
    - Preserve a regra de negócio.
    """,
    expected_output="Plano de refatoração SQL com sugestões de correção.",
    agent=agente_refatorador_sql,
    context=[tarefa_diagnostico],
    output_file="docs/plano_refatoracao_queries_v2.md"
)

tarefa_versionamento = Task(
    description="""
    Validar os arquivos:
    - docs/diagnostico_queries_v2.md
    - docs/plano_refatoracao_queries_v2.md

    Se existirem, execute git_manager_tool com a mensagem:
    Diagnostico e plano v2 para correcao de queries SQL
    """,
    expected_output="Relatório de versionamento.",
    agent=agente_versionamento,
    context=[tarefa_diagnostico, tarefa_refatoracao]
)

crew_corretor_v2 = Crew(
    agents=[
        agente_diagnostico_sql,
        agente_refatorador_sql,
        agente_versionamento,
    ],
    tasks=[
        tarefa_diagnostico,
        tarefa_refatoracao,
        tarefa_versionamento,
    ],
    process=Process.sequential,
    verbose=True
)

if __name__ == "__main__":
    print("### Iniciando Corretor de Queries V2 ###")
    resultado = crew_corretor_v2.kickoff()
    print(resultado)