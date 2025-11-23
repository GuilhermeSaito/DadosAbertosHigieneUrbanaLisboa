import requests
import pandas as pd
import os

def get_osm_ecopontos():
    url = "https://overpass-api.de/api/interpreter"
    
    # Query em Overpass QL
    # 1. [out:json]; -> Define o formato de saída
    # 2. area["name"="Lisboa"]->.searchArea; -> Define a área de busca
    # 3. node["amenity"="recycling"](area.searchArea); -> Busca pontos de reciclagem nessa área
    # 4. out body; -> Retorna os dados
    query = """
    [out:json][timeout:25];
    area["name"="Lisboa"]->.searchArea;
    (
      node["amenity"="recycling"](area.searchArea);
    );
    out body;
    """
    
    print(f"Enviando consulta para o Overpass API...")
    
    try:
        response = requests.get(url, params={'data': query})
        response.raise_for_status()
        
        data_json = response.json()
        
        # O OSM retorna uma lista chamada "elements" [cite: 8]
        if 'elements' in data_json and len(data_json['elements']) > 0:
            rows = []
            for element in data_json['elements']:
                # Dados básicos
                row = {
                    'osm_id': element.get('id'),
                    'lat': element.get('lat'),  # Latitude (y) [cite: 8]
                    'lon': element.get('lon')   # Longitude (x) [cite: 8]
                }
                
                # Desempacotar as 'tags' (onde estão os detalhes do lixo) [cite: 9, 11]
                if 'tags' in element:
                    for key, value in element['tags'].items():
                        # Adiciona cada tag como uma coluna (ex: amenity, recycling_type)
                        row[key] = value
                        
                rows.append(row)
            
            df = pd.DataFrame(rows)
            print(f"Sucesso! {len(df)} pontos de reciclagem encontrados no OSM.")
            return df
        else:
            print("Nenhum dado encontrado ou estrutura inesperada.")
            return None
            
    except Exception as e:
        print(f"Erro na requisição: {e}")
        return None

df_osm = get_osm_ecopontos()

if df_osm is not None:
    output_path = os.path.join("..", "..", "dados", "bronze", "osm_ecopontos.csv")
    
    # Salvar em CSV
    df_osm.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"Arquivo salvo em: {output_path}")