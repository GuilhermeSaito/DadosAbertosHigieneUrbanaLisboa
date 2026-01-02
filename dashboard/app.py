import streamlit as st
import pandas as pd
import altair as alt
import os

# --- 1. CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="Monitorização Lisboa 360º",
    page_icon="♻️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 2. CARREGAMENTO DE DADOS ROBUSTO ---
@st.cache_data
def load_data():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    possible_paths = [
        os.path.join(base_dir, "..", "dados", "ouro", "tabela_analitica_final.parquet"), 
        os.path.join("dados", "ouro", "tabela_analitica_final.parquet"), 
        "/opt/airflow/dados/ouro/tabela_analitica_final.parquet" 
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            try:
                df = pd.read_parquet(path)
                if 'data_referencia' in df.columns:
                    df['data_referencia'] = pd.to_datetime(df['data_referencia'])
                
                # Normalização de nomes
                cols_map = {'populacao_residente_2021': 'populacao_residente_distrito'}
                df.rename(columns=cols_map, inplace=True)
                return df
            except Exception as e:
                st.error(f"Erro ao ler o arquivo: {e}")
                return None
            
    return None

df = load_data()

if df is None:
    st.error("🚨 Arquivo de dados não encontrado.")
    st.stop()

# --- 3. BARRA LATERAL (FILTROS) ---
st.sidebar.title("🎛️ Controlo")

# Filtro de Data
if 'data_referencia' in df.columns and not df['data_referencia'].isna().all():
    min_date = df['data_referencia'].min().date()
    max_date = df['data_referencia'].max().date()
    start_date, end_date = st.sidebar.date_input(
        "📅 Período:", [min_date, max_date], min_value=min_date, max_value=max_date
    )
else:
    st.stop()

# Filtro de Freguesia
all_freguesias = sorted(df['freguesia'].dropna().unique())
freguesias_sel = st.sidebar.multiselect(
    "📍 Freguesias:", all_freguesias, default=all_freguesias[:5]
)

# Filtro de Dia da Semana
dia_tipo = st.sidebar.radio("📆 Dias:", ["Todos", "Dias Úteis", "Fim de Semana"])

# Botão de Download (NOVO!)
if not df.empty:
    st.sidebar.divider()
    st.sidebar.download_button(
        label="📥 Baixar Dados Filtrados (CSV)",
        data=df.to_csv(index=False).encode('utf-8'),
        file_name='dados_lisboa_filtrados.csv',
        mime='text/csv',
    )

# --- 4. APLICAÇÃO DOS FILTROS ---
mask = (df['data_referencia'].dt.date >= start_date) & (df['data_referencia'].dt.date <= end_date)

if freguesias_sel:
    mask &= (df['freguesia'].isin(freguesias_sel))

if dia_tipo == "Dias Úteis":
    mask &= (df['is_fim_de_semana'] == 0)
elif dia_tipo == "Fim de Semana":
    mask &= (df['is_fim_de_semana'] == 1)

df_filtered = df[mask].copy()

if df_filtered.empty:
    st.warning("⚠️ Nenhum dado encontrado para os filtros selecionados.")
    st.stop()

# --- 5. KPIs GERAIS ---
st.title("♻️ Monitorização de Higiene Urbana")
st.markdown(f"**Análise de {len(freguesias_sel) if freguesias_sel else 'Toda a Cidade'}** | {start_date.strftime('%d/%m/%Y')} a {end_date.strftime('%d/%m/%Y')}")

kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

total_queixas = df_filtered['total_queixas_diarias'].sum()
media_diaria = df_filtered.groupby('data_referencia')['total_queixas_diarias'].sum().mean()
pop_total_sel = df_filtered.groupby('freguesia')['populacao_residente_distrito'].max().sum()
queixas_por_1k = (total_queixas / pop_total_sel) * 1000 if pop_total_sel > 0 else 0

# (NOVO KPI) Capacidade per Capita
cap_total = df_filtered.groupby('freguesia')['capacidade_instalada_litros'].max().sum()
litros_por_hab = cap_total / pop_total_sel if pop_total_sel > 0 else 0

kpi1.metric("Total Queixas", f"{int(total_queixas)}")
kpi2.metric("Média Diária", f"{media_diaria:.1f}")
kpi3.metric("Incidência /1k hab", f"{queixas_por_1k:.2f}")
kpi4.metric("População", f"{int(pop_total_sel):,}")
kpi5.metric("Capacidade/Hab", f"{litros_por_hab:.1f} L", help="Quantos litros de lixo a infraestrutura suporta por habitante")

st.divider()

# --- 6. ABAS DE ANÁLISE ---
tab_op, tab_infra, tab_clima, tab_sobre = st.tabs(["📊 Operação & Tendências", "🏗️ Infraestrutura & Capacidade", "🌧️ Clima", "ℹ️ Sobre"])

# === ABA 1: OPERAÇÃO ===
with tab_op:
    col_a, col_b = st.columns([2, 1])
    
    with col_a:
        st.subheader("Evolução Temporal e Tendência")
        
        daily = df_filtered.groupby('data_referencia')[['total_queixas_diarias', 'queixas_recolha', 'queixas_pragas']].sum().reset_index()
        
        # (NOVO) Média Móvel de 7 dias para suavizar a linha
        daily['media_movel_7d'] = daily['total_queixas_diarias'].rolling(window=7).mean()
        
        # Melt para o gráfico de área
        daily_melt = daily.melt(
            id_vars=['data_referencia', 'media_movel_7d'],
            value_vars=['queixas_recolha', 'queixas_pragas'],
            var_name='Tipo',
            value_name='Quantidade'
        )
        
        # Camada 1: Área empilhada (Detalhe)
        area = alt.Chart(daily_melt).mark_area(opacity=0.4).encode(
            x=alt.X('data_referencia:T', title='Data'),
            y=alt.Y('Quantidade:Q', stack=True, title='Nº Queixas'),
            color=alt.Color('Tipo:N', title='Motivo')
        )
        
        # Camada 2: Linha de Tendência (Média Móvel) - Destaque!
        line_trend = alt.Chart(daily).mark_line(color='black', strokeDash=[5, 5], size=2).encode(
            x='data_referencia:T',
            y='media_movel_7d:Q',
            tooltip=[alt.Tooltip('media_movel_7d:Q', title='Média Móvel (7d)', format='.1f')]
        )

        st.altair_chart((area + line_trend).interactive(), use_container_width=True)
        st.caption("A linha tracejada preta indica a tendência semanal (Média Móvel 7 dias).")

    with col_b:
        st.subheader("Ranking de Freguesias")
        top_freg = df_filtered.groupby('freguesia')['total_queixas_diarias'].sum().sort_values(ascending=True).tail(10).reset_index()
        
        chart_top = alt.Chart(top_freg).mark_bar(color="#FF4B4B").encode(
            x=alt.X('total_queixas_diarias:Q', title='Total'),
            y=alt.Y('freguesia:N', sort='-x', title=None),
            tooltip=['freguesia', 'total_queixas_diarias']
        ).properties(height=400)
        
        st.altair_chart(chart_top, use_container_width=True)

# === ABA 2: INFRAESTRUTURA ===
with tab_infra:
    st.info("💡 **Análise de Capacidade:** Freguesias com menos litros disponíveis por habitante sofrem mais?")
    
    agg_infra = df_filtered.groupby('freguesia').agg({
        'num_ecopontos': 'max',
        'capacidade_instalada_litros': 'max',
        'populacao_residente_distrito': 'max',
        'total_queixas_diarias': 'sum'
    }).reset_index()
    
    # Métricas calculadas
    agg_infra['queixas_por_1k'] = (agg_infra['total_queixas_diarias'] / agg_infra['populacao_residente_distrito']) * 1000
    agg_infra['litros_por_hab'] = agg_infra['capacidade_instalada_litros'] / agg_infra['populacao_residente_distrito']
    
    col_x, col_y = st.columns(2)
    
    with col_x:
        st.subheader("Capacidade (L/hab) vs Queixas")
        # Scatter Plot: Capacidade vs Reclamações
        scatter = alt.Chart(agg_infra).mark_circle(size=120).encode(
            x=alt.X('litros_por_hab:Q', title='Capacidade Instalada (Litros/Hab)'),
            y=alt.Y('queixas_por_1k:Q', title='Queixas (por 1k hab)'),
            color=alt.Color('freguesia:N', legend=None),
            tooltip=['freguesia', 'litros_por_hab', 'queixas_por_1k', 'num_ecopontos']
        ).interactive()
        
        # Linha média de referência
        rule = alt.Chart(agg_infra).mark_rule(color='red').encode(x='mean(litros_por_hab):Q')
        
        st.altair_chart(scatter + rule, use_container_width=True)
        st.caption("A linha vermelha é a média da cidade. Freguesias à esquerda têm infraestrutura abaixo da média.")

    with col_y:
        st.subheader("Mapa de Infraestrutura")
        infra_melt = agg_infra.melt(
            id_vars=['freguesia'], 
            value_vars=['litros_por_hab', 'queixas_por_1k'],
            var_name='Indicador', 
            value_name='Valor'
        )
        
        chart_comp = alt.Chart(infra_melt).mark_bar().encode(
            x=alt.X('freguesia:N', sort='-y', title=None),
            y=alt.Y('Valor:Q'),
            color=alt.Color('Indicador:N', scale=alt.Scale(scheme='set2')),
            column=alt.Column('Indicador:N', header=alt.Header(title=None)),
            tooltip=['freguesia', 'Valor']
        ).resolve_scale(y='independent')
        
        st.altair_chart(chart_comp, use_container_width=True)

# === ABA 3: CLIMA ===
with tab_clima:
    st.subheader("🌧️ Chuva vs Ocorrências")
    
    clima_dia = df_filtered.groupby('data_referencia').agg({
        'precipitacao_total_mm': 'mean',
        'total_queixas_diarias': 'sum',
        'temp_media': 'mean'
    }).reset_index()
    
    base = alt.Chart(clima_dia).encode(x='data_referencia:T')
    
    bar_chuva = base.mark_bar(opacity=0.3, color='#4c78a8').encode(
        y=alt.Y('precipitacao_total_mm:Q', title='Chuva (mm)'),
        tooltip=['data_referencia', 'precipitacao_total_mm']
    )
    
    line_queixas = base.mark_line(color='#e45756', strokeWidth=3).encode(
        y=alt.Y('total_queixas_diarias:Q', title='Total Queixas'),
        tooltip=['data_referencia', 'total_queixas_diarias']
    )
    
    st.altair_chart(alt.layer(bar_chuva, line_queixas).resolve_scale(y='independent').properties(height=400), use_container_width=True)
    
    # Correlação simples
    corr = clima_dia['precipitacao_total_mm'].corr(clima_dia['total_queixas_diarias'])
    st.info(f"📊 Correlação estatística entre Chuva e Queixas: **{corr:.2f}** (Escala -1 a 1)")

# === ABA 4: SOBRE ===
with tab_sobre:
    st.markdown("""
    ### ℹ️ Sobre este Painel
    Este dashboard faz parte do projeto de **Engenharia de Dados - Higiene Urbana de Lisboa**.
    
    **Fontes de Dados:**
    1.  **Ocorrências:** Portal "Na Minha Rua" (Dados Abertos CML).
    2.  **Infraestrutura:** Cadastro de Ecopontos e Circuitos (ArcGIS CML).
    3.  **Demografia:** Censos 2021 (INE).
    4.  **Clima:** API Open-Meteo (Histórico).
    
    **Glossário de Métricas:**
    * **Incidência /1k hab:** Número de reclamações normalizado pelo tamanho da população. Permite comparar freguesias grandes e pequenas de forma justa.
    * **Capacidade/Hab:** Quantidade total de litros (somatório do volume dos ecopontos) dividida pela população residente.
    * **Média Móvel (7d):** Média das queixas nos últimos 7 dias. Ajuda a identificar tendências reais ignorando picos diários isolados.
    """)