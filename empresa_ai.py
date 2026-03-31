import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from crewai import Agent, Task, Crew, Process, LLM
from langchain_ollama import OllamaLLM
from toolsformyagents import git_manager_tool, file_validator_tool, ler_arquivo_csv
from dotenv import load_dotenv
import os

# Carrega variáveis do .env
load_dotenv()

# Aqui está a mágica: ele vai direto no localhost:11434 e procura o llama3.2
llm_local = LLM(
    model="ollama/llama3.2",
    base_url="http://localhost:11434"
)

# Lista de arquivos corrigida (com vírgula e extensões)
arquivos_faturamento = [
    r"data/select_faturamento.xlsx - BANCO_QUERY.csv",
    r"data/select_faturamento.xlsx - TB_GLOSAS.csv",
    r"data/select_faturamento.xlsx - TB_DEMANDAS_EXTERNAS.csv",
    r"data/select_faturamento.xlsx - QUADRO_ORCAMENTO_25_26.csv",
    r"data/select_faturamento.xlsx - QUADRO_ITENS_ORCADO.csv"
]

# 2. Definindo os Agentes
arquiteto_mis = Agent(
    role='Arquiteto de Soluções MIS e Backend',
    goal='Projetar a estrutura do banco de dados (PostgreSQL) e a arquitetura em Python.',
    backstory='Especialista em modelagem no DBeaver/PgAdmin e automação de operações de faturamento.',
    llm=llm_local,
    tools=[ler_arquivo_csv], # <--- ADICIONE ESTA LINHA AQUI
    verbose=True
)

desenvolvedor_web = Agent(
    role='Engenheiro de Software Web e Relatórios',
    goal='Criar a interface do usuário e implementar exportação de PDF/Excel.',
    backstory='Programador Python focado em Pandas, Flask/Streamlit e integração com Postgres.',
    llm=llm_local,
    verbose=True
)

analista_dados = Agent(
    role='Git Manager e Analista de Integridade',
    goal='Validar a documentação gerada e sincronizar o progresso com o GitHub.',
    backstory='Você garante que nenhum código seja perdido e que o repositório esteja sempre atualizado.',
    tools=[file_validator_tool, git_manager_tool, ler_arquivo_csv],
    llm=llm_local, # ADICIONADO AQUI
    verbose=True
)

# 3. Definindo as Tarefas (Sem duplicidade)
tarefa_arquitetura = Task(
    description=f"""
    Analise os esquemas de dados presentes nos arquivos: {arquivos_faturamento}.
    1. Projete as tabelas no PostgreSQL para 'TB_GLOSAS', 'TB_DEMANDAS_EXTERNAS' e 'QUADRO_ORCAMENTO'.
    2. Crie o script SQL 'init_db.sql' para criação dessas tabelas.
    3. Garanta que as queries do 'BANCO_QUERY' funcionem nesta estrutura.
    """,
    expected_output='Documento de arquitetura técnica + script SQL de criação de tabelas.',
    agent=arquiteto_mis,
    output_file='arquitetura_e_sql.md'
)

tarefa_codigo_inicial = Task(
    description='''
    Com base no SQL gerado, crie o código Python para conectar no banco local.
    Crie uma rotina que importe os dados dos CSVs enviados para as novas tabelas do Postgres.
    Desenvolva o esqueleto de uma interface Dashboard.
    ''',
    expected_output='Código Python completo (app_faturamento.py) com conexão ao banco.',
    agent=desenvolvedor_web,
    context=[tarefa_arquitetura],
    output_file='app_faturamento.py'
)

tarefa_versionamento = Task(
    description='''
    1. Use o file_validator_tool para verificar se 'arquitetura_e_sql.md' e 'app_faturamento.py' existem.
    2. Use o git_manager_tool para fazer o push desses arquivos com a mensagem 'Desenvolvimento inicial do Portal de Faturamento'.
    ''',
    expected_output='Confirmação de que os arquivos estão salvos e enviados ao GitHub.',
    agent=analista_dados
)

# 4. Montando a Equipe
empresa_desenvolvimento = Crew(
    agents=[arquiteto_mis, desenvolvedor_web, analista_dados],
    tasks=[tarefa_arquitetura, tarefa_codigo_inicial, tarefa_versionamento],
    process=Process.sequential,
    verbose=True
)

# 5. Execução
if __name__ == "__main__":
    print("### Iniciando Desenvolvimento e Versionamento Automático ###")
    resultado = empresa_desenvolvimento.kickoff()
    print("######################")
    print("TRABALHO CONCLUÍDO E SINCRONIZADO!")