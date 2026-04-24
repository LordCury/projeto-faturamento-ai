import os

from dotenv import load_dotenv
from crewai import Agent, Task, Crew, Process, LLM
from toolsformyagents import (
    git_manager_tool,
    file_validator_tool,
    ler_arquivo_csv,
    ler_catalogo_queries_tool,
    extrair_canais_catalogo_tool,
    buscar_query_por_canal_tool,
)

load_dotenv(override=True)

llm_local = LLM(
    model=os.getenv("MODEL", "ollama/llama3.2"),
    base_url=os.getenv("BASE_URL", "http://localhost:11434")
)

agente_arquitetura = Agent(
    role="Arquiteto de Dados e Soluções Analíticas",
    goal="Analisar o catálogo de queries e a estrutura dos dados para apoiar ETL, correção de queries e dashboards.",
    backstory="Especialista em modelagem analítica, SQL, legado Power Query e arquitetura de dados.",
    llm=llm_local,
    tools=[
        ler_arquivo_csv,
        ler_catalogo_queries_tool,
        extrair_canais_catalogo_tool,
        buscar_query_por_canal_tool,
    ],
    verbose=True
)

agente_integridade = Agent(
    role="Analista de Integridade e Versionamento",
    goal="Validar arquivos e registrar o progresso do projeto no Git.",
    backstory="Especialista em validação de artefatos, organização e versionamento.",
    llm=llm_local,
    tools=[file_validator_tool, git_manager_tool],
    verbose=True
)

tarefa_catalogo_queries = Task(
    description="""
    Use extrair_canais_catalogo_tool para identificar os canais presentes em data/BANCO_QUERY.csv.

    Depois, para os canais encontrados, use buscar_query_por_canal_tool
    e faça uma análise técnica canal a canal.

    Objetivos:
    1. Identificar os canais.
    2. Identificar padrões das queries.
    3. Identificar tabelas, aliases, CTEs e regras de negócio.
    4. Identificar sinais de legado Power Query / Microsoft.
    5. Gerar documentação técnica.

    Não invente tabelas ou colunas que não apareçam nas tools.
    """,
    expected_output="Documento técnico com análise do catálogo de queries.",
    agent=agente_arquitetura,
    output_file="docs/arquitetura_e_sql.md"
)

tarefa_correcao_queries = Task(
    description="""
    Analise o catálogo de queries e as queries corrigidas.

    Objetivos:
    1. Identificar queries que ainda apresentam erro.
    2. Classificar o tipo de erro:
       - placeholder não substituído
       - aspas inválidas
       - encoding
       - sintaxe SQL
       - regra de negócio
    3. Propor ajustes preservando a regra original.
    4. Documentar as correções necessárias.

    Não sobrescreva a query original do catálogo.
    Trabalhe sobre as versões corrigidas em arquivos .sql.
    """,
    expected_output="Documento com análise dos erros e plano de correção das queries.",
    agent=agente_arquitetura,
    context=[tarefa_catalogo_queries],
    output_file="docs/correcao_queries.md"
)

tarefa_validacao_git = Task(
    description="""
    Validar os arquivos:
    - extractor.py
    - query_pipeline.py
    - empresa_ai.py
    - app_faturamento.py
    - toolsformyagents.py
    - docs/arquitetura_e_sql.md
    - docs/correcao_queries.md

    Se existirem, executar git_manager_tool com a mensagem:
    Estrutura inicial robusta do portal de faturamento

    Informar claramente se houve sucesso ou falha.
    """,
    expected_output="Relatório de validação e status do Git.",
    agent=agente_integridade,
    context=[tarefa_catalogo_queries, tarefa_correcao_queries]
)

crew_portal_faturamento = Crew(
    agents=[agente_arquitetura, agente_integridade],
    tasks=[
        tarefa_catalogo_queries,
        tarefa_correcao_queries,
        tarefa_validacao_git
    ],
    process=Process.sequential,
    verbose=True
)

if __name__ == "__main__":
    print("### Iniciando Crew do Portal de Faturamento ###")
    try:
        resultado = crew_portal_faturamento.kickoff()
        print(resultado)
    except Exception as e:
        print(f"Erro durante a execução da crew: {e}")