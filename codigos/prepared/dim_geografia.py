import duckdb
import os

# Caminhos (ajuste conforme a sua estrutura de pastas)
BRONZE_PATH = os.path.join("..", "..", "dados", "bronze")
SILVER_PATH = os.path.join("..", "..", "dados", "silver")

# Garante que a pasta silver existe
os.makedirs(SILVER_PATH, exist_ok=True)

# Conexão com o DuckDB (arquivo persistente para servir de Data Warehouse local)
db_path = os.path.join(SILVER_PATH, "lisboa_waste_dw.duckdb")
con = duckdb.connect(db_path)

print(f"--- INICIANDO PROCESSAMENTO CAMADA PRATA (DUCKDB) ---")

# ==============================================================================
# 1. FUNÇÕES AUXILIARES (UDFs)
# ==============================================================================
# Função para normalizar nomes de freguesia (remove acentos, minúsculas, espaços extras)
# Isso é CRUCIAL para fazer o JOIN entre fontes diferentes (ex: 'Beato' vs 'Beato ')
con.execute("""
    CREATE OR REPLACE MACRO normalizar_texto(texto) AS
    lower(strip_accents(trim(texto)));
""")

# ==============================================================================
# 2. DIMENSÃO GEOGRAFIA & DEMOGRAFIA (CORRIGIDO)
# ==============================================================================
print("Processando: Dimensão Geografia...")

# Carrega GeoAPI (já estava correto)
geo_df_path = os.path.join(BRONZE_PATH, "geoapi_distrito_lisboa.csv")
con.execute(f"CREATE OR REPLACE TEMP TABLE raw_geo AS SELECT * FROM read_csv_auto('{geo_df_path}')")

# Carrega INE (População) - Atenção aos nomes das colunas geradas pelo seu script 'ine.py'
pop_path = os.path.join(BRONZE_PATH, "populacao_idade_sexo_censo_ine.csv")
con.execute(f"CREATE OR REPLACE TEMP TABLE raw_pop AS SELECT * FROM read_csv_auto('{pop_path}')")

# Criação da Tabela Silver com o JOIN ATIVO
# Assumindo que no CSV do INE a coluna de ligação seja 'geocod' ou 'cod_regiao' e o valor seja 'valor'
con.execute("""
    CREATE OR REPLACE TABLE silver_dim_geografia AS
    SELECT 
        g.codigoine,
        g.distrito,
        g.concelho,
        g.freguesia,
        normalizar_texto(g.freguesia) as freguesia_norm,
        
        -- Tenta pegar a população total (ajuste 'p.valor' para o nome real da coluna no seu CSV)
        CAST(p.populacao_total AS INT) as populacao_residente_distrito
        
    FROM raw_geo g
    -- Faz o JOIN pelo código INE (garanta que ambos são strings ou inteiros)
    LEFT JOIN (
        SELECT 
            geocod, 
            SUM(valor) as populacao_total
        FROM raw_pop
        WHERE 
            -- Filtra Sexo = Total ('T' ou 'HM')
            (dim_3 = 'T' OR dim_3_t = 'HM')
            -- E TAMBÉM filtra Idade = Total e Escolaridade = Total para evitar duplicação
            AND dim_4 = 'T' AND dim_5 = 'T'
        GROUP BY geocod
    ) p ON CAST(g.codigoine AS VARCHAR) = CAST(p.geocod AS VARCHAR)
    WHERE p.geocod IS NOT NULL -- Filtro opcional para garantir integridade
""")
print(" -> Tabela 'silver_dim_geografia' criada com dados demográficos.")

# ==============================================================================
# 3. OCORRÊNCIAS "NA MINHA RUA"
# ==============================================================================
# Objetivo: Filtrar Higiene Urbana, limpar datas e vincular à freguesia [cite: 790, 817]

print("Processando: Ocorrências...")
oc_path = os.path.join(BRONZE_PATH, "ocorrencias_minha_rua.csv")

con.execute(f"""
    CREATE OR REPLACE TABLE silver_fact_ocorrencias AS
    SELECT
        CAST(dt_registo AS TIMESTAMP) as data_ocorrencia,
        tipo AS tipo,
        Freguesia AS freguesia,
        Latitude_Subseccao AS latitude,
        Longitude_Subseccao AS longitude,
        
        -- Normalização para chave estrangeira
        normalizar_texto("Freguesia") as freguesia_join_key,
        
        -- Flag booleana para facilitar contagem na Gold
        CASE WHEN tipo ILIKE '%recolha%' THEN 1 ELSE 0 END as is_pedido_recolha,
        CASE WHEN tipo ILIKE '%pragas%' OR tipo ILIKE '%doenças%' THEN 1 ELSE 0 END as is_pragas
        
    FROM read_csv_auto('{oc_path}')
    WHERE 
        -- Filtro definido no documento 'ObjetivoProjeto' 
        tipo IS NOT NULL 
        -- Adicione filtros de categoria se existirem colunas como 'Categoria' = 'Higiene Urbana'
""")
print(" -> Tabela 'silver_fact_ocorrencias' criada.")

# ==============================================================================
# 4. INFRAESTRUTURA CML (ECOPONTOS)
# ==============================================================================
# Objetivo: Limpar dados de ecopontos oficiais [cite: 436]

print("Processando: Ecopontos CML...")
cml_path = os.path.join(BRONZE_PATH, "ecopontos_cml.csv")
circuitos_path_ecopontos = os.path.join(BRONZE_PATH, "circuitos_recolha.csv")

con.execute(f"""
    CREATE OR REPLACE TABLE silver_dim_ecopontos_cml AS
    SELECT
        e.OBJECTID as id_cml,
        e.TPRS_DESC as tipo_ecoponto,
        e.TOP_MOD_1 as morada,
        e.FRE_AB as nome_freguesia_original,
        normalizar_texto(e.FRE_AB) as freguesia_join_key,
        e.y as latitude,
        e.x as longitude,
        CAST(c.CONTENTOR_TOTAL_LITROS AS INT) as capacidade_litros,
        c.CONTENTOR_RESIDUO as tipo_residuo_contentor
        
    FROM read_csv_auto('{cml_path}') e
    -- Fazemos o JOIN com a tabela de circuitos usando o código único do local (COD_SIG)
    LEFT JOIN read_csv_auto('{circuitos_path_ecopontos}') c 
        ON CAST(e.COD_SIG AS VARCHAR) = CAST(c.COD_SIG AS VARCHAR)
""")
print(" -> Tabela 'silver_dim_ecopontos_cml' criada.")

# ==============================================================================
# 5. METEOROLOGIA (OPEN METEO)
# ==============================================================================
# Objetivo: Padronizar dados climáticos históricos [cite: 810]

print("Processando: Meteorologia...")
meteo_path = os.path.join(BRONZE_PATH, "open_meteo_distrito_lisboa.csv")

con.execute(f"""
    CREATE OR REPLACE TABLE silver_fact_meteo AS
    SELECT
        CAST(datetime AS DATE) as data_referencia,
        concelho,
        temperatura_c,
        chuva_mm,
        vento_kmh,
        umidade_relativa
    FROM read_csv_auto('{meteo_path}')
""")
print(" -> Tabela 'silver_fact_meteo' criada.")

# ==============================================================================
# 5.1. CIRCUITOS DE RECOLHA (NOVA TABELA)
# ==============================================================================
# ==============================================================================
# 5.1. CIRCUITOS DE RECOLHA (CORRIGIDO)
# ==============================================================================
print("Processando: Circuitos de Recolha...")
circuitos_path = os.path.join(BRONZE_PATH, "circuitos_recolha.csv")

con.execute(f"""
    CREATE OR REPLACE TABLE silver_dim_circuitos_recolha AS
    SELECT
        CIRCUITO_CODIGO as id_circuito,
        CIRCUITO_RESIDUO as tipo_residuo,
        CIRCUITO_REGRA as frequencia_texto, 
        CIRCUITO_PERIODICIDADE as periodicidade_desc,
        CIRCUITO_HORA_INICIO as horario_coleta,
        lower(trim(CIRCUITO_REGRA)) as frequencia_norm,
        
        -- Lógica de Categoria: Primeiro tentamos a coluna oficial, depois o texto da regra
        CASE 
            WHEN CIRCUITO_PERIODICIDADE ILIKE '%Diário%' THEN 'Diario'
            WHEN CIRCUITO_REGRA ILIKE '%2ª%' THEN 'Dias Uteis'
            WHEN CIRCUITO_REGRA ILIKE '%3ª%' THEN 'Dias Uteis'
            WHEN CIRCUITO_REGRA ILIKE '%4ª%' THEN 'Dias Uteis'
            WHEN CIRCUITO_REGRA ILIKE '%5ª%' THEN 'Dias Uteis'
            WHEN CIRCUITO_REGRA ILIKE '%domingo%' OR CIRCUITO_REGRA ILIKE '%sábado%' THEN 'Fim de Semana'
            ELSE 'Outros' 
        END as periodicidade_categoria
        
    FROM read_csv_auto('{circuitos_path}')
""")
print(" -> Tabela 'silver_dim_circuitos_recolha' criada.")

# ==============================================================================
# 6. INFRAESTRUTURA COMUNITÁRIA (OSM)
# ==============================================================================
# Objetivo: Limpar dados do OSM. [cite: 450]
# Nota: Como o OSM não tem a coluna 'Freguesia', na Gold layer precisaremos
# fazer um join espacial ou usar a latitude/longitude aproximada.

print("Processando: Ecopontos OSM...")
osm_path = os.path.join(BRONZE_PATH, "osm_ecopontos.csv")

con.execute(f"""
    CREATE OR REPLACE TABLE silver_dim_ecopontos_osm AS
    SELECT
        osm_id,
        lat as latitude,
        lon as longitude,
        amenity,
        recycling_type,
            
        CAST(CASE 
            WHEN lower(COALESCE(CAST("recycling:glass" AS VARCHAR), CAST("recycling:glass_bottles" AS VARCHAR), 'no')) IN ('yes', 'true', '1') THEN 1 
            ELSE 0 
        END AS BOOLEAN) as recicla_vidro,
        
        -- PADRONIZAÇÃO DO PAPEL
        CAST(CASE 
            WHEN lower(COALESCE(CAST("recycling:paper" AS VARCHAR), CAST("recycling:cardboard" AS VARCHAR), 'no')) IN ('yes', 'true', '1') THEN 1 
            ELSE 0 
        END AS BOOLEAN) as recicla_papel,
        
        -- PADRONIZAÇÃO DO PLÁSTICO
        CAST(CASE 
            WHEN lower(COALESCE(CAST("recycling:plastic" AS VARCHAR), CAST("recycling:plastic_bottles" AS VARCHAR), CAST("recycling:plastic_packaging" AS VARCHAR), 'no')) IN ('yes', 'true', '1') THEN 1 
            ELSE 0 
        END AS BOOLEAN) as recicla_plastico
    FROM read_csv_auto('{osm_path}')
""")

print(" -> Tabela 'silver_dim_ecopontos_osm' criada.")

# ==============================================================================
# VALIDAÇÃO FINAL
# ==============================================================================
# print("\n--- RESUMO DA CARGA SILVER ---")
# tables = ["silver_dim_geografia", "silver_fact_ocorrencias", "silver_dim_ecopontos_cml", "silver_fact_meteo", "silver_dim_ecopontos_osm"]

# for t in tables:
#     count = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
#     print(f"Tabela {t}: {count} linhas")

# Exportar para Parquet (Opcional, mas recomendado para Data Lake)
# con.execute("COPY silver_fact_ocorrencias TO '../../dados/silver/fact_ocorrencias.parquet' (FORMAT PARQUET)")

con.close()
print("\nProcessamento Silver concluído com sucesso!")