import streamlit as st
from empresa_ai import listar_tabelas_schema, listar_logs_etl, executar_consulta
from extractor import ETLExtractor

st.set_page_config(page_title="Portal de Faturamento", layout="wide")

st.title("Portal Analítico de Faturamento")

menu = st.sidebar.radio(
    "Menu",
    ["Dashboard", "ETL", "Logs ETL", "Tabelas Staging", "Consulta SQL"]
)

if menu == "Dashboard":
    st.subheader("Dashboard")
    st.info("Área inicial do dashboard. Aqui vamos colocar gráficos e indicadores.")

    try:
        df_logs = listar_logs_etl()
        df_tabelas = listar_tabelas_schema()

        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total de logs carregados", len(df_logs))
        with col2:
            st.metric("Total de tabelas no schema", len(df_tabelas))

        st.write("Últimos logs da ETL")
        st.dataframe(df_logs.head(10), use_container_width=True)

    except Exception as e:
        st.error(f"Erro ao carregar informações do dashboard: {e}")

elif menu == "ETL":
    st.subheader("Execução ETL")

    modo_carga = st.selectbox(
        "Modo de carga",
        ["replace", "append", "truncate_append"]
    )

    incremental = st.checkbox("Carga incremental", value=True)
    dias_retroativos = st.number_input(
        "Dias retroativos",
        min_value=1,
        max_value=30,
        value=1
    )

    if st.button("Executar ETL"):
        try:
            extractor = ETLExtractor()
            extractor.executar_todos(
                modo_carga=modo_carga,
                incremental=incremental,
                dias_retroativos=dias_retroativos
            )
            st.success("ETL executada. Verifique os logs.")
        except Exception as e:
            st.error(f"Erro ao executar a ETL: {e}")

elif menu == "Logs ETL":
    st.subheader("Logs da ETL")
    try:
        df_logs = listar_logs_etl()
        st.dataframe(df_logs, use_container_width=True)
    except Exception as e:
        st.error(f"Erro ao carregar logs: {e}")

elif menu == "Tabelas Staging":
    st.subheader("Tabelas do Schema")
    try:
        df_tabelas = listar_tabelas_schema()
        st.dataframe(df_tabelas, use_container_width=True)
    except Exception as e:
        st.error(f"Erro ao listar tabelas: {e}")

elif menu == "Consulta SQL":
    st.subheader("Consulta SQL")
    st.warning("Use apenas consultas SELECT neste ambiente.")

    query = st.text_area("Digite a query", height=200)

    if st.button("Executar consulta"):
        try:
            if not query.strip():
                st.warning("Digite uma query.")
            elif not query.strip().lower().startswith("select"):
                st.error("Por segurança, apenas consultas SELECT são permitidas.")
            else:
                df = executar_consulta(query)
                st.dataframe(df, use_container_width=True)
        except Exception as e:
            st.error(f"Erro na consulta: {e}")