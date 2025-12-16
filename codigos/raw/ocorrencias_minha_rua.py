import requests
import pandas as pd
import io
import os
from geoapi_freguesias import salvar_no_minio

def get_ocorrencias_lisboa():
    # ID do dataset conforme a URL do portal (https://dados.cm-lisboa.pt/dataset/ocorrencias-na-minha-rua)
    dataset_slug = "ocorrencias-na-minha-rua"
    
    # Endpoint da API do CKAN para ler os metadados do pacote
    api_url = f"https://dados.cm-lisboa.pt/api/3/action/package_show?id={dataset_slug}"

    print(f"Consultando API do Lisboa Aberta para: {dataset_slug}...")
    
    try:
        # 1. Obter os metadados para descobrir o link atual do arquivo
        response = requests.get(api_url)
        response.raise_for_status()
        data_json = response.json()
        
        if not data_json.get('success'):
            print("A API retornou insucesso na busca do dataset.")
            return None
            
        resources = data_json['result']['resources']
        
        # 2. Procurar o recurso que seja Excel (XLSX ou XLS)
        target_resource = None
        for res in resources:
            # Verifica o formato declarado (pode ser XLSX, XLS, Excel, etc.)
            fmt = res.get('format', '').lower()
            if fmt in ['xlsx', 'xls', 'excel']:
                target_resource = res
                break
        
        # Fallback: Se não achar excel, tenta CSV
        if not target_resource:
            for res in resources:
                if res.get('format', '').lower() == 'csv':
                    target_resource = res
                    break

        if target_resource:
            file_url = target_resource['url']
            file_fmt = target_resource['format']
            print(f"Recurso encontrado: {target_resource['name']} (Formato: {file_fmt})")
            print(f"Baixando arquivo de: {file_url} ...")
            
            # 3. Baixar o arquivo binário
            file_response = requests.get(file_url)
            file_response.raise_for_status()
            
            # 4. Ler o arquivo em memória com Pandas
            print("Processando arquivo (isso pode demorar dependendo do tamanho)...")
            try:
                # Usa io.BytesIO para ler os bytes baixados como se fosse um arquivo
                if 'xls' in file_fmt.lower():
                    df = pd.read_excel(io.BytesIO(file_response.content))
                else:
                    df = pd.read_csv(io.BytesIO(file_response.content))
                
                print(f"Sucesso! {len(df)} ocorrências carregadas.")
                return df
                
            except Exception as e_read:
                print(f"Erro ao converter o arquivo para DataFrame: {e_read}")
                print("Dica: Verifique se instalou o 'openpyxl' (pip install openpyxl).")
                return None
        else:
            print("Nenhum recurso Excel ou CSV válido encontrado neste dataset.")
            return None

    except Exception as e:
        print(f"Erro na requisição: {e}")
        return None
    
# geo_df_path = os.path.join("..", "..", "dados", "bronze", "ocorrencias_minha_rua.csv")
# df_ocorrencias = pd.read_csv(geo_df_path)

df_ocorrencias = get_ocorrencias_lisboa()

if df_ocorrencias is not None:
    print("Resgatando os dados de ocorrencias")
    # # Salva como CSV padronizado na nossa pasta bronze
    # output_path = os.path.join("..", "..", "dados", "bronze", "ocorrencias_minha_rua.csv")
    
    # # Salvar
    # df_ocorrencias.to_csv(output_path, index=False, encoding='utf-8-sig')
    # print(f"Arquivo salvo em: {output_path}")

    salvar_no_minio(df_ocorrencias, "ocorrencias_minha_rua.csv")