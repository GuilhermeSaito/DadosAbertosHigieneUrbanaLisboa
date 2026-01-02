import requests
import pandas as pd
import os
import time
from geoapi_freguesias import salvar_no_minio

def get_service_layers(service_url):
    # A URL de metadados é a base + '?f=json'
    meta_url = f"{service_url}?f=json"
    print(f"🔍 Consultando estrutura do serviço: {meta_url}")
    
    try:
        response = requests.get(meta_url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if 'layers' in data:
            layers = data['layers']
            print(f"   ✅ Encontradas {len(layers)} layers disponíveis:")
            for l in layers:
                print(f"      - ID {l['id']}: {l['name']}")
            return layers
        else:
            print("   ⚠️ Nenhuma informação de 'layers' encontrada neste serviço.")
            return []
            
    except Exception as e:
        print(f"   ❌ Erro ao consultar metadados: {e}")
        return []

def get_dados_arcgis_layer(url_layer, layer_name="Desconhecido"):
    all_rows = []
    offset = 0
    step = 1000 

    print(f"   -> Baixando dados da Layer: {layer_name} ...")

    while True:
        params = {
            "where": "1=1",           
            "outFields": "*",         
            "f": "json",              
            "resultOffset": offset,
            "resultRecordCount": step
        }
        
        try:
            response = requests.get(url_layer, params=params, timeout=30)
            
            # Se a layer não existir ou der erro 400, capturamos aqui sem quebrar o script todo
            if response.status_code != 200:
                print(f"      [Aviso] Erro HTTP {response.status_code} na layer {layer_name}. Pulando.")
                break
                
            data_json = response.json()
            
            if 'error' in data_json:
                print(f"      [Erro API] {data_json['error'].get('message', 'Erro desconhecido')}")
                break

            if 'features' in data_json and len(data_json['features']) > 0:
                features = data_json['features']
                count = len(features)
                
                for feature in features:
                    row = feature['attributes']
                    if 'geometry' in feature:
                        row.update(feature['geometry'])
                    
                    # Adiciona colunas de rastreio
                    row['layer_nome'] = layer_name
                    row['dataset_origem'] = 'CML_ArcGIS'
                    
                    all_rows.append(row)

                if count < step:
                    break 
                offset += step
                time.sleep(0.5) 
            else:
                break
                
        except Exception as e:
            print(f"      [Erro Crítico] {e}")
            break
        
    return all_rows

def get_ecopontos_completo():
    """
    Descobre dinamicamente as layers e baixa tudo.
    """
    todos_ecopontos = []
    
    # Lista de Serviços para varrer (Superfície e Subterrâneo)
    servicos = [
        "https://services.arcgis.com/1dSrzEWVQn5kHHyK/arcgis/rest/services/Amb_Reciclagem/FeatureServer",
        "https://services.arcgis.com/1dSrzEWVQn5kHHyK/arcgis/rest/services/Amb_Ecopontos_Subterraneos/FeatureServer"
    ]

    for base_url in servicos:
        # 1. Descobre o que existe
        layers_disponiveis = get_service_layers(base_url)
        
        # 2. Itera apenas sobre o que existe
        for layer in layers_disponiveis:
            layer_id = layer['id']
            layer_nome = layer['name']
            
            # Monta a URL de download
            url_query = f"{base_url}/{layer_id}/query"
            
            # Baixa
            dados = get_dados_arcgis_layer(url_query, layer_name=layer_nome)
            todos_ecopontos.extend(dados)

    if len(todos_ecopontos) > 0:
        df = pd.DataFrame(todos_ecopontos)
        print(f"\nTOTAL FINAL: {len(df)} ecopontos recuperados.")
        return df
    else:
        return None

def get_circuitos():
    url = "https://services.arcgis.com/1dSrzEWVQn5kHHyK/arcgis/rest/services/CircuitosContentores/FeatureServer/0/query"
    # Circuitos geralmente é fixo na layer 0, mas podemos usar a mesma lógica se quiser
    dados = get_dados_arcgis_layer(url, layer_name="Circuitos_Recolha")
    if dados:
        return pd.DataFrame(dados)
    return None

# --- Execução Principal ---
if __name__ == "__main__":
    
    print("--- INICIANDO PROCESSO CML ---")
    
    # 1. Ecopontos
    df_ecopontos = get_ecopontos_completo()
    if df_ecopontos is not None:
        salvar_no_minio(df_ecopontos, "ecopontos_cml.csv")
        # print("Salvando no csv")
        # output_path_ecopontos = os.path.join("..", "..", "dados", "bronze", "ecopontos_cml.csv")
        # df_ecopontos.to_csv(output_path_ecopontos, index=False, encoding='utf-8-sig')
    
    # 2. Circuitos
    df_circuitos = get_circuitos()
    if df_circuitos is not None:
        salvar_no_minio(df_circuitos, "circuitos_recolha.csv")
        # print("Salvando no csv")
        # output_path_circuito = os.path.join("..", "..", "dados", "bronze", "circuitos_recolha.csv")
        # df_circuitos.to_csv(output_path_circuito, index=False, encoding='utf-8-sig')
        
    print("--- FIM PROCESSO CML ---")