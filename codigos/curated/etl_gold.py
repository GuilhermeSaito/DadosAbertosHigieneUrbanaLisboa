import duckdb
import os

# Caminhos
SILVER_PATH = os.path.join("..", "..", "dados", "silver")
GOLD_PATH = os.path.join("..", "..", "dados", "gold")

# Garante que a pasta gold existe
os.makedirs(GOLD_PATH, exist_ok=True)

# Conecta ao MESMO banco da camada prata (vamos ler de lá e escrever as tabelas gold lá também)
db_path = os.path.join(SILVER_PATH, "lisboa_waste_dw.duckdb")
con = duckdb.connect(db_path)

print(f"--- INICIANDO PROCESSAMENTO CAMADA OURO (DUCKDB) ---")

# ==============================================================================
# 1. DIMENSÃO CALENDÁRIO
# ==============================================================================
# Cria uma lista de datas de 2023 até o fim de 2025 para servir de eixo temporal
print("Gerando: Dimensão Calendário...")
con.execute("""
    CREATE OR REPLACE TABLE gold_dim_calendario AS
    SELECT 
        CAST(range AS DATE) as data_referencia,
        dayname(range) as dia_semana,
        month(range) as mes,
        year(range) as ano,
        -- Flag para saber se é fim de semana (Sábado=6, Domingo=0 no DuckDB dependendo da config, ou usar isodow)
        CASE WHEN dayofweek(range) IN (0, 6) THEN 1 ELSE 0 END as is_fim_de_semana
    FROM range(DATE '2023-01-01', DATE '2025-12-31', INTERVAL 1 DAY)
""")

# ==============================================================================
# 2. AGREGADO DE INFRAESTRUTURA (OFERTA)
# ==============================================================================
# Agrupa os ecopontos por freguesia para saber a capacidade total instalada
print("Agregando: Infraestrutura CML por Freguesia...")
con.execute("""
    CREATE OR REPLACE TEMP TABLE tmp_infra_agg AS
    SELECT
        freguesia_join_key,
        COUNT(*) as num_ecopontos_cml,
        -- Se capacidade for nula, assume 0 para a soma
        SUM(COALESCE(capacidade_litros, 0)) as capacidade_total_litros
    FROM silver_dim_ecopontos_cml
    GROUP BY freguesia_join_key
""")

# ==============================================================================
# 3. AGREGADO DE OCORRÊNCIAS (FALHAS)
# ==============================================================================
# Agrupa reclamações por Dia e Freguesia
print("Agregando: Ocorrências Diárias...")
con.execute("""
    CREATE OR REPLACE TEMP TABLE tmp_ocorrencias_agg AS
    SELECT
        CAST(data_ocorrencia AS DATE) as data_ref,
        freguesia_join_key,
        COUNT(*) as total_queixas,
        SUM(is_pedido_recolha) as total_pedidos_recolha,
        SUM(is_pragas) as total_queixas_pragas
    FROM silver_fact_ocorrencias
    GROUP BY 1, 2
""")

# ==============================================================================
# 4. AGREGADO DE CLIMA (CONTEXTO)
# ==============================================================================
# O clima vem por Concelho (ex: Lisboa, Amadora). Precisamos agrupar por dia.
print("Agregando: Meteorologia Diária...")
con.execute("""
    CREATE OR REPLACE TEMP TABLE tmp_meteo_agg AS
    SELECT
        data_referencia,
        -- Normaliza o nome do concelho para garantir o join com a geografia
        normalizar_texto(concelho) as concelho_norm,
        AVG(temperatura_c) as temp_media,
        MAX(temperatura_c) as temp_max,
        SUM(chuva_mm) as precipitacao_total_mm
    FROM silver_fact_meteo
    GROUP BY 1, 2
""")

# ==============================================================================
# 5. TABELA FATO FINAL (O PRODUTO DE DADOS)
# ==============================================================================
# Aqui fazemos o cruzamento de TODAS as dimensões.
# Estratégia: 
# 1. Cross Join de Calendário x Geografia (Garante 1 linha para cada freguesia em cada dia)
# 2. Left Join com Ocorrências (traz falhas do dia)
# 3. Left Join com Infraestrutura (traz dados estáticos da freguesia)
# 4. Left Join com Meteo (traz clima do concelho naquele dia)

print("Construindo: gold_fact_analise_diaria (O Tabelão Final)...")

con.execute("""
    CREATE OR REPLACE TABLE gold_fact_analise_diaria AS
    SELECT
        -- Tempo
        cal.data_referencia,
        cal.dia_semana,
        cal.is_fim_de_semana,
        
        -- Geografia
        geo.concelho,
        geo.freguesia,
        geo.populacao_residente_distrito,
        
        -- Infraestrutura (Oferta)
        COALESCE(inf.num_ecopontos_cml, 0) as num_ecopontos,
        COALESCE(inf.capacidade_total_litros, 0) as capacidade_instalada_litros,
        
        -- KPI: Habitantes por Ecoponto (Quanto menor, melhor)
        CASE 
            WHEN inf.num_ecopontos_cml > 0 THEN CAST(geo.populacao_residente_distrito AS FLOAT) / inf.num_ecopontos_cml 
            ELSE NULL 
        END as ratio_hab_por_ecoponto,
        
        -- Ocorrências (Falhas - Se não tiver registro no dia, é 0)
        COALESCE(occ.total_queixas, 0) as total_queixas_diarias,
        COALESCE(occ.total_pedidos_recolha, 0) as queixas_recolha,
        COALESCE(occ.total_queixas_pragas, 0) as queixas_pragas,
        
        -- Clima (Contexto)
        met.temp_media,
        met.precipitacao_total_mm
        
    FROM gold_dim_calendario cal
    
    -- 1. Explode para todas as freguesias (Backbone)
    CROSS JOIN silver_dim_geografia geo
    
    -- 2. Traz Ocorrências
    LEFT JOIN tmp_ocorrencias_agg occ 
        ON cal.data_referencia = occ.data_ref 
        AND geo.freguesia_norm = occ.freguesia_join_key
        
    -- 3. Traz Infraestrutura (Join estático pela freguesia)
    LEFT JOIN tmp_infra_agg inf 
        ON geo.freguesia_norm = inf.freguesia_join_key
        
    -- 4. Traz Clima (Join por Data + Concelho)
    LEFT JOIN tmp_meteo_agg met
        ON cal.data_referencia = met.data_referencia
        AND normalizar_texto(geo.concelho) = met.concelho_norm
        
    ORDER BY cal.data_referencia DESC, geo.freguesia ASC
""")

print(" -> Tabela 'gold_fact_analise_diaria' criada com sucesso!")

# ==============================================================================
# EXPORTAÇÃO PARA ANALYTICS (CSV/PARQUET)
# ==============================================================================
# Gera um arquivo físico na pasta Gold para ser consumido por PowerBI/Tableau/Excel
output_file = os.path.join(GOLD_PATH, "tabela_analitica_final.parquet")
con.execute(f"COPY gold_fact_analise_diaria TO '{output_file}' (FORMAT PARQUET)")
print(f" -> Arquivo final exportado para: {output_file}")

# Validação Simples
print("\n--- AMOSTRA DOS DADOS (TOP 5) ---")
print(con.execute("SELECT data_referencia, freguesia, num_ecopontos, total_queixas_diarias FROM gold_fact_analise_diaria WHERE total_queixas_diarias > 0 LIMIT 5").fetchdf())

con.close()
print("\nProcessamento Gold concluído!")