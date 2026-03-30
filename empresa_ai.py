from crewai import Agent, Task, Crew, Process, LLM

# Conectando ao Ollama do jeito novo e oficial do CrewAI
llm_local = LLM(
    model="ollama/llama3.2",
    base_url="http://localhost:11434"
)

# 2. Definindo os Agentes (Sua Equipe)
ceo = Agent(
    role='Diretor de MIS e Operações',
    goal='Estratégia, planejamento de KPIs e estruturação de equipes de dados para Call Center.',
    backstory='Você tem 15 anos de experiência em operações de Call Center e TI. Você pensa em métricas de negócio, eficiência, TMA, SLA e estruturação de banco de dados (PostgreSQL).',
    llm=llm_local,
    verbose=True,
    allow_delegation=True
)

analista = Agent(
    role='Analista de Dados Sênior',
    goal='Executar tarefas técnicas, criar consultas SQL, scripts Python e documentação técnica.',
    backstory='Você é um especialista técnico em Python, SQL (DBeaver/pgAdmin) e Power BI. Você transforma os planos do Diretor de MIS em realidade técnica.',
    llm=llm_local,
    verbose=True,
    allow_delegation=False
)

# 3. Definindo as Tarefas
tarefa_planejamento = Task(
    description='Crie um plano de contratação e estrutura para uma nova equipe de MIS focada em dados. O plano deve incluir: 3 cargos essenciais, ferramentas que serão usadas (ex: PostgreSQL, Power BI, Python) e 5 KPIs operacionais que essa equipe deverá monitorar diariamente.',
    expected_output='Um documento formatado em Markdown com o plano completo.',
    agent=ceo
)

tarefa_documentacao = Task(
    description='A partir do plano criado pelo Diretor, formate um documento final profissional e salve o resultado em um arquivo.',
    expected_output='O documento final salvo no disco.',
    agent=analista,
    output_file='plano_equipe_mis.md' # É AQUI QUE O ARQUIVO FÍSICO É GERADO!
)

# 4. Montando a "Empresa" (Crew)
minha_empresa = Crew(
    agents=[ceo, analista],
    tasks=[tarefa_planejamento, tarefa_documentacao],
    process=Process.sequential, # Um trabalha depois do outro
    verbose=True
)

# 5. Iniciando o trabalho
print("### Iniciando Operação da Empresa de IA ###")
resultado = minha_empresa.kickoff()

print("######################")
print("TRABALHO CONCLUÍDO! Verifique o arquivo 'plano_equipe_mis.md' na sua pasta.")