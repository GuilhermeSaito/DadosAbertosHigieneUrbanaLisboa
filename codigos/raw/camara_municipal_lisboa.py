import requests
import pandas as pd
import os

def get_dados_camara_municipal_lisboa(url):
    url = url
    all_rows = []
    offset = 0
    step = 1000

    while True:
        params = {
            "where": "1=1",           # Pega tudo
            "outFields": "*",         # Todas as colunas
            "f": "json",              # Formato JSON padrão do ArcGIS
            "resultOffset": offset,
            "resultRecordCount": step
        }

        print(f"Conectando a: {url} ...")
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data_json = response.json()
            
            # Verificação de erro na estrutura
            if 'error' in data_json:
                print("A API retornou um erro lógico:")
                print(data_json['error'])
                return None

            if 'features' in data_json and len(data_json['features']) > 0:
                features = data_json['features']
                count = len(features)
                print(f"Baixando lote: registros {offset} a {offset + count}...")
                for feature in features:
                    row = feature['attributes']
                    if 'geometry' in feature:
                        row.update(feature['geometry']) 
                    all_rows.append(row)

                if count < step:
                    break

                offset += step
            else:
                print("A resposta não contém 'features'. Conteúdo recebido (início):")
                # Imprime os primeiros 200 caracteres para entendermos o que veio
                print(str(data_json)[:200])
                break
                
        except Exception as e:
            print(f"Erro crítico na requisição: {e}")
            return None
        
    if len(all_rows) > 0:
        df = pd.DataFrame(all_rows)
        print(f"\nSUCESSO: {len(df)} registros carregados.")
        return df
    else:
        print("Nenhum dado foi encontrado.")
        return None

# --- Execução e Salvamento ---
url = "https://services.arcgis.com/1dSrzEWVQn5kHHyK/arcgis/rest/services/Amb_Reciclagem/FeatureServer/2/query"
df_ecopontos = get_dados_camara_municipal_lisboa(url)
if df_ecopontos is not None:
    print("Resgatando os dados dos ECOPONTOS")
    output_path_ecopontos = os.path.join("..", "..", "dados", "bronze", "ecopontos_cml.csv")
    
    df_ecopontos.to_csv(output_path_ecopontos, index=False, encoding='utf-8-sig')
    print(f"Arquivo salvo em: {output_path_ecopontos}")

url = "https://services.arcgis.com/1dSrzEWVQn5kHHyK/arcgis/rest/services/CircuitosContentores/FeatureServer/0/query"
df_circuito_recolha = get_dados_camara_municipal_lisboa(url)
if df_circuito_recolha is not None:
    print("Resgatando os dados dos CIRTUICOS")
    output_path_circuito_recolha = os.path.join("..", "..", "dados", "bronze", "circuitos_recolha.csv")
    
    df_circuito_recolha.to_csv(output_path_circuito_recolha, index=False, encoding='utf-8-sig')
    print(f"Arquivo salvo em: {output_path_circuito_recolha}")