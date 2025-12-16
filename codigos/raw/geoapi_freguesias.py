import requests
import pandas as pd
import os
import time

def get_lisbon_district_full_structure():
    url_base_distrito = "https://geoapi.pt/distrito/lisboa/municipios?json=1"
    
    print(f"--- FASE 1: Buscando municípios do Distrito de Lisboa ---")
    print(f"URL: {url_base_distrito}")
    
    try:
        response = requests.get(url_base_distrito)
        response.raise_for_status()
        data_distrito = response.json()
        
        # Obtém a lista bruta
        lista_municipios_raw = data_distrito.get('municipios', [])
        
        if not lista_municipios_raw:
            print("Erro: A lista de municípios veio vazia.")
            return None
            
        print(f"Municípios encontrados: {len(lista_municipios_raw)}")
        
        dados_consolidados = []
        
        # --- FASE 2: Iterar com validação de tipo ---
        for i, item in enumerate(lista_municipios_raw):
            
            # LÓGICA DE CORREÇÃO: Extrair o nome corretamente
            if isinstance(item, dict):
                concelho_nome = item.get('nome')
                # Às vezes o INE já vem aqui, o que nos poupa uma chamada!
                codigo_ine_concelho = item.get('codigoine', item.get('ine', None))
            else:
                concelho_nome = str(item)
                codigo_ine_concelho = None
            
            print(f"[{i+1}/{len(lista_municipios_raw)}] Processando: {concelho_nome}...")
            
            if not concelho_nome:
                continue

            # Se não pegamos o INE na lista, buscamos no detalhe
            if not codigo_ine_concelho:
                try:
                    url_meta = f"https://geoapi.pt/municipio/{concelho_nome}?json=1"
                    resp_meta = requests.get(url_meta)
                    if resp_meta.status_code == 200:
                        meta_data = resp_meta.json()
                        codigo_ine_concelho = meta_data.get('codigoine', meta_data.get('ine', 'N/A'))
                except:
                    codigo_ine_concelho = "N/A"

            # Buscar Freguesias
            try:
                url_freg = f"https://geoapi.pt/municipio/{concelho_nome}/freguesias?json=1"
                resp_freg = requests.get(url_freg)
                
                lista_freguesias = []
                if resp_freg.status_code == 200:
                    freg_data = resp_freg.json()
                    if isinstance(freg_data, list):
                        lista_freguesias = freg_data
                    elif isinstance(freg_data, dict):
                        # Tenta pegar chaves comuns de resposta
                        lista_freguesias = freg_data.get('freguesias', freg_data.get('results', []))

                # Montar linhas
                if lista_freguesias:
                    for freg in lista_freguesias:
                        dados_consolidados.append({
                            "codigoine": codigo_ine_concelho,
                            "distrito": "LISBOA",
                            "concelho": concelho_nome.upper(),
                            "freguesia": freg
                        })
                else:
                    # Fallback se não tiver freguesias (raro)
                    dados_consolidados.append({
                        "codigoine": codigo_ine_concelho,
                        "distrito": "LISBOA",
                        "concelho": concelho_nome.upper(),
                        "freguesia": "N/A"
                    })
                
                time.sleep(0.5)

            except Exception as e_inner:
                print(f"Erro ao buscar freguesias de {concelho_nome}: {e_inner}")

        # Criar DataFrame final
        if dados_consolidados:
            df = pd.DataFrame(dados_consolidados)
            
            # Ordenação segura das colunas
            cols_order = ["codigoine", "distrito", "concelho", "freguesia"]
            # Garante que as colunas existem antes de reordenar (evita o erro crítico)
            for col in cols_order:
                if col not in df.columns:
                    df[col] = None
            
            df = df[cols_order]
            
            print(f"\nSUCESSO: {len(df)} registros processados.")
            return df
        else:
            print("Nenhum dado consolidado foi gerado.")
            return None

    except Exception as e:
        print(f"Erro crítico: {e}")
        return None
    
def salvar_no_minio(df, nome_arquivo):
    endpoint_url = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "admin12345")
    bucket_name = "bronze"

    caminho_s3 = f"s3://{bucket_name}/{nome_arquivo}"
    
    print(f"Salvando no MinIO ({caminho_s3})...")
    
    try:
        df.to_csv(
            caminho_s3,
            index=False,
            encoding='utf-8-sig',
            storage_options={
                "key": access_key,
                "secret": secret_key,
                "client_kwargs": {
                    "endpoint_url": endpoint_url
                }
            }
        )
        print("Upload concluído com sucesso!")
    except Exception as e:
        print(f"ERRO ao salvar no MinIO: {e}")
        print("Dica: Verifique se o bucket 'bronze' foi criado no painel do MinIO (localhost:9001).")

if __name__ == "__main__":
    # geo_df_path = os.path.join("..", "..", "dados", "bronze", "geoapi_distrito_lisboa.csv")
    # df_distrito_completo = pd.read_csv(geo_df_path)


    df_distrito_completo = get_lisbon_district_full_structure()

    if df_distrito_completo is not None:
        print(f"Resgatando os dados de GEOAPI")
        # output_path = os.path.join("..", "..", "dados", "bronze", "geoapi_distrito_lisboa.csv")
        
        # df_distrito_completo.to_csv(output_path, index=False, encoding='utf-8-sig')
        # print(f"Arquivo salvo em: {output_path}")
        salvar_no_minio(df_distrito_completo, "geoapi_distrito_lisboa.csv")