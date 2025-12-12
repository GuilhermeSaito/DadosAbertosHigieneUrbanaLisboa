import requests
import pandas as pd
import os
import time
from datetime import datetime, timedelta

# --- 1. Nova Função de Geocoding (OpenStreetMap) ---
def get_coords_nominatim(concelho_nome):
    url = "https://nominatim.openstreetmap.org/search"
    
    # Adicionando 'Lisboa, Portugal' para garantir que não pegamos cidades homônimas no Brasil ou outros lugares
    query = f"{concelho_nome}, Lisboa, Portugal"
    params = {
        "q": query,
        "format": "json",
        "limit": 1
    }
    
    # É obrigatório identificar o seu script/app para o Nominatim não bloquear
    headers = {
        "User-Agent": "EngenhariaDados_Lisboa_Project/1.0"
    }
    
    try:
        response = requests.get(url, params=params, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            if data:
                # O Nominatim retorna 'lat' e 'lon' como strings
                return float(data[0]['lat']), float(data[0]['lon'])
    except Exception as e:
        print(f"Erro no geocoding de {concelho_nome}: {e}")
    
    return None, None

def get_weather_batch(lat, lon, location_name, start_year, end_year):
    """Baixa o histórico do Open-Meteo para uma coordenada específica."""
    url = "https://archive-api.open-meteo.com/v1/archive"
    
    start_date = f"{start_year}-01-01"
    if end_year >= datetime.now().year:
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    else:
        end_date = f"{end_year}-12-31"

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,relative_humidity_2m,rain,wind_speed_10m",
        "timezone": "Europe/Lisbon"
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        hourly = data.get('hourly', {})
        if hourly:
            df = pd.DataFrame({
                'datetime': hourly['time'],
                'temperatura_c': hourly['temperature_2m'],
                'chuva_mm': hourly['rain'],
                'vento_kmh': hourly['wind_speed_10m'],
                'umidade_relativa': hourly['relative_humidity_2m']
            })
            df['concelho'] = location_name
            return df
    except Exception as e:
        print(f"Erro ao baixar meteo para {location_name}: {e}")
    
    return None

# --- Fluxo Principal ---

def main_meteo_distributed():
    path_geoapi = os.path.join("..", "..", "dados", "bronze", "geoapi_distrito_lisboa.csv")
    df_geo = pd.read_csv(path_geoapi)
    
    lista_concelhos = df_geo['concelho'].dropna().unique()
    print(f"Iniciando extração para {len(lista_concelhos)} concelhos via Nominatim...")
    
    all_weather_dfs = []
    for concelho in lista_concelhos:
        print(f"\nLocalizando: {concelho}...")
        
        lat, lon = get_coords_nominatim(concelho)
        # Pausa de 1s é importante para respeitar as regras do Nominatim (política de uso)
        time.sleep(1)
        
        if lat and lon:
            print(f"   -> Encontrado: {lat:.4f}, {lon:.4f}")
            # B. Baixar Clima (2023 a 2025)
            # Damos mais uma pausa para o Open-Meteo
            time.sleep(1) 
            df_concelho = get_weather_batch(lat, lon, concelho, 2023, 2025)
            
            if df_concelho is not None:
                all_weather_dfs.append(df_concelho)
                print(f"   -> {len(df_concelho)} registos meteorológicos baixados.")
        else:
            print(f"   -> Coordenadas não encontradas.")

    # 3. Consolidar e Salvar
    if all_weather_dfs:
        df_final = pd.concat(all_weather_dfs, ignore_index=True)
        
        output_path = os.path.join("..", "..", "dados", "bronze", "open_meteo_distrito_lisboa.csv")    
        df_final.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        print("\n" + "="*50)
        print(f"SUCESSO TOTAL! Arquivo salvo em: {output_path}")
        print(f"Total de linhas: {len(df_final)}")
    else:
        print("Nenhum dado foi coletado.")

# Executar
main_meteo_distributed()