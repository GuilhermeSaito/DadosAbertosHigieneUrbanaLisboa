import duckdb
import os

# --- CONFIGURAÇÃO DE CAMINHOS ---
# O Banco de Dados (Output) continua sendo salvo localmente no volume do container
# para persistência e performance.
SILVER_PATH = os.path.join("..", "..", "dados", "prata")
os.makedirs(SILVER_PATH, exist_ok=True)
db_path = os.path.join(SILVER_PATH, "lisboa_waste_dw.duckdb")

# Conexão com o DuckDB
con = duckdb.connect(db_path)

print(f"--- INICIANDO PROCESSAMENTO CAMADA PRATA (VIA MINIO/S3) ---")

# ==============================================================================
# 0. CONFIGURAÇÃO S3 / MINIO
# ==============================================================================
# Instala e carrega a extensão para ler S3
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")

# Define as credenciais do MinIO (baseadas no seu docker-compose.yml)
# s3_url_style='path' é obrigatório para MinIO
con.execute("""
    SET s3_region='us-east-1';
    SET s3_url_style='path';
    SET s3_endpoint='minio:9000';
    SET s3_access_key_id='admin';
    SET s3_secret_access_key='admin12345';
    SET s3_use_ssl=false;
""")

# URL base do Bucket Bronze
BRONZE_BUCKET = "s3://bronze"

# ==============================================================================
# 1. MACROS
# ==============================================================================
con.execute("""
    CREATE OR REPLACE MACRO normalizar_texto(texto) AS
    lower(strip_accents(trim(texto)));
""")

# ==============================================================================
# 2. DIMENSÃO GEOGRAFIA & DEMOGRAFIA
# ==============================================================================
print("Processando: Dimensão Geografia...")

# Nota: Ajustei para 'populacao_escolaridade' pois sua query usa dim_5, 
# que só existe nesse arquivo (idade_sexo geralmente vai só até dim_4).
con.execute(f"CREATE OR REPLACE TEMP TABLE raw_geo AS SELECT * FROM read_csv_auto('{BRONZE_BUCKET}/geoapi_distrito_lisboa.csv')")
con.execute(f"CREATE OR REPLACE TEMP TABLE raw_pop AS SELECT * FROM read_csv_auto('{BRONZE_BUCKET}/populacao_escolaridade_censo_ine.csv')")

con.execute("""
    CREATE OR REPLACE TABLE silver_dim_geografia AS
    SELECT 
        g.codigoine,
        g.distrito,
        g.concelho,
        g.freguesia,
        normalizar_texto(g.freguesia) as freguesia_norm,
        CAST(p.populacao_total AS INT) as populacao_residente_distrito
    FROM raw_geo g
    LEFT JOIN (
        SELECT 
            geocod, 
            SUM(valor) as populacao_total
        FROM raw_pop
        WHERE 
            (dim_3 = 'T' OR dim_3_t = 'HM')
            AND dim_4 = 'T' AND dim_5 = 'T'
        GROUP BY geocod
    ) p ON CAST(g.codigoine AS VARCHAR) = CAST(p.geocod AS VARCHAR)
    WHERE p.geocod IS NOT NULL
""")
print(" -> Tabela 'silver_dim_geografia' criada.")

# ==============================================================================
# 3. OCORRÊNCIAS "NA MINHA RUA"
# ==============================================================================
print("Processando: Ocorrências...")

con.execute(f"""
    CREATE OR REPLACE TABLE silver_fact_ocorrencias AS
    SELECT
        CAST(dt_registo AS TIMESTAMP) as data_ocorrencia,
        tipo AS tipo,
        Freguesia AS freguesia,
        Latitude_Subseccao AS latitude,
        Longitude_Subseccao AS longitude,
        normalizar_texto("Freguesia") as freguesia_join_key,
        CASE WHEN tipo ILIKE '%recolha%' THEN 1 ELSE 0 END as is_pedido_recolha,
        CASE WHEN tipo ILIKE '%pragas%' OR tipo ILIKE '%doenças%' THEN 1 ELSE 0 END as is_pragas
    FROM read_csv_auto('{BRONZE_BUCKET}/ocorrencias_minha_rua.csv')
    WHERE tipo IS NOT NULL 
""")
print(" -> Tabela 'silver_fact_ocorrencias' criada.")

# ==============================================================================
# 4. INFRAESTRUTURA CML (ECOPONTOS)
# ==============================================================================
print("Processando: Ecopontos CML...")

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
    FROM read_csv_auto('{BRONZE_BUCKET}/ecopontos_cml.csv') e
    LEFT JOIN read_csv_auto('{BRONZE_BUCKET}/circuitos_recolha.csv') c 
        ON CAST(e.COD_SIG AS VARCHAR) = CAST(c.COD_SIG AS VARCHAR)
""")
print(" -> Tabela 'silver_dim_ecopontos_cml' criada.")

# ==============================================================================
# 5. METEOROLOGIA (OPEN METEO)
# ==============================================================================
print("Processando: Meteorologia...")

con.execute(f"""
    CREATE OR REPLACE TABLE silver_fact_meteo AS
    SELECT
        CAST(datetime AS DATE) as data_referencia,
        concelho,
        temperatura_c,
        chuva_mm,
        vento_kmh,
        umidade_relativa
    FROM read_csv_auto('{BRONZE_BUCKET}/open_meteo_distrito_lisboa.csv')
""")
print(" -> Tabela 'silver_fact_meteo' criada.")

# ==============================================================================
# 5.1. CIRCUITOS DE RECOLHA
# ==============================================================================
print("Processando: Circuitos de Recolha...")

con.execute(f"""
    CREATE OR REPLACE TABLE silver_dim_circuitos_recolha AS
    SELECT
        CIRCUITO_CODIGO as id_circuito,
        CIRCUITO_RESIDUO as tipo_residuo,
        CIRCUITO_REGRA as frequencia_texto, 
        CIRCUITO_PERIODICIDADE as periodicidade_desc,
        CIRCUITO_HORA_INICIO as horario_coleta,
        lower(trim(CIRCUITO_REGRA)) as frequencia_norm,
        CASE 
            WHEN CIRCUITO_PERIODICIDADE ILIKE '%Diário%' THEN 'Diario'
            WHEN CIRCUITO_REGRA ILIKE '%2ª%' THEN 'Dias Uteis'
            WHEN CIRCUITO_REGRA ILIKE '%domingo%' OR CIRCUITO_REGRA ILIKE '%sábado%' THEN 'Fim de Semana'
            ELSE 'Outros' 
        END as periodicidade_categoria
    FROM read_csv_auto('{BRONZE_BUCKET}/circuitos_recolha.csv')
""")
print(" -> Tabela 'silver_dim_circuitos_recolha' criada.")

# ==============================================================================
# 6. INFRAESTRUTURA COMUNITÁRIA (OSM)
# ==============================================================================
print("Processando: Ecopontos OSM...")

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
        CAST(CASE 
            WHEN lower(COALESCE(CAST("recycling:paper" AS VARCHAR), CAST("recycling:cardboard" AS VARCHAR), 'no')) IN ('yes', 'true', '1') THEN 1 
            ELSE 0 
        END AS BOOLEAN) as recicla_papel,
        CAST(CASE 
            WHEN lower(COALESCE(CAST("recycling:plastic" AS VARCHAR), CAST("recycling:plastic_bottles" AS VARCHAR), CAST("recycling:plastic_packaging" AS VARCHAR), 'no')) IN ('yes', 'true', '1') THEN 1 
            ELSE 0 
        END AS BOOLEAN) as recicla_plastico
    FROM read_csv_auto('{BRONZE_BUCKET}/osm_ecopontos.csv')
""")
print(" -> Tabela 'silver_dim_ecopontos_osm' criada.")

# Validação final (opcional)
tables = ["silver_dim_geografia", "silver_fact_ocorrencias", "silver_dim_ecopontos_cml", "silver_fact_meteo", "silver_dim_ecopontos_osm"]
for t in tables:
    try:
        count = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        print(f"✅ {t}: {count} linhas")
    except Exception as e:
        print(f"⚠️ Erro ao verificar {t}: {e}")

con.close()
print("\nProcessamento Silver (S3) concluído com sucesso!")