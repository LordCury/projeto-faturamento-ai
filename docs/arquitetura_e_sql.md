# Arquitetura Inicial do Portal de Faturamento

## Objetivo
Criar uma aplicação web em Python para executar ETLs baseadas em catálogo de queries,
persistir dados no PostgreSQL e disponibilizar dashboards, logs e análises.

## Componentes iniciais
- `extractor.py`: motor ETL
- `empresa_ai.py`: acesso ao banco
- `app_faturamento.py`: aplicação web Streamlit
- `toolsformyagents.py`: tools dos agentes
- `crew_portal_faturamento.py`: orquestração da crew

## Banco de dados
Schema principal: `empresa_ai`

## Tabela inicial de log
- `empresa_ai.etl_execucoes_log`

## Próximos passos
- consolidar staging por canal
- criar fatos e dimensões
- criar dashboards gerenciais
- evoluir previsões