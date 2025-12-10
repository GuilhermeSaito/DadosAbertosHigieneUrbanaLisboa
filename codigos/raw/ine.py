import requests
import pandas as pd
import os
import time

def get_ine_data_api(codigo_indicador, cod_regiao):
    url = "https://www.ine.pt/ine/json_indicador/pindica.jsp"
    
    params = {
        "op": "2",              
        "varcd": codigo_indicador,
        "lang": "PT",
        "Dim2": cod_regiao
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status() 
        
        data_json = response.json()

        # if codigo_indicador == "0011638" and cod_regiao == "17":
        #     print(data_json)
        
        # Tenta converter o JSON para DataFrame.
        # A estrutura do INE geralmente é uma lista onde o primeiro item contém 'Dados'.
        # Se for uma lista simples, o pandas converte direto.
        try:
            if isinstance(data_json, list) and 'Dados' in data_json[0]:
                # Estrutura comum do INE onde os dados reais estão aninhados
                dados_reais = data_json[0]['Dados']
                # As vezes 'Dados' é um dict por ano, precisamos tratar:
                if isinstance(dados_reais, dict):
                    # Exemplo: achatar os dados de todos os anos disponíveis
                    lista_final = []
                    for ano, lista_valores in dados_reais.items():
                        for item in lista_valores:
                            item['ano_referencia'] = ano # Adiciona coluna de ano
                            lista_final.append(item)
                    df = pd.DataFrame(lista_final)
                else:
                    df = pd.DataFrame(dados_reais)
            else:
                # Tentativa genérica
                df = pd.DataFrame(data_json)
                
            return df
            
        except Exception as parse_error:
            print(f"[AVISO] Erro ao converter JSON para DataFrame: {parse_error}")
            # Se não der para virar DF, retorna o JSON bruto para debug
            return data_json
        
    except Exception as e:
        print(f"[ERRO] Falha ao conectar ao INE para o código {codigo_indicador}: {e}")
        return None


# =============================================================================
# EXECUÇÃO PRINCIPAL
# =============================================================================
indicadores = {
    "populacao_idade_sexo": "0011638",
    "edificios": "0012582",
    "familias": "0011697"
}
REGIOES = {
    "11": "Norte",
    "16": "Centro",
    "17": "AreaMetropolitanaLisboa",
    "18": "Alentejo",
    "15": "Algarve",
    "20": "Acores",
    "30": "Madeira"
}

for nome, codigo in indicadores.items():
    print(f"--- Processando: {nome} (Código: {codigo}) ---")

    buffer_dados = []
    # Itera sobre cada região para baixar pedaço por pedaço
    for cod_regiao, nome_regiao in REGIOES.items():
        print(f"   -> Baixando {nome_regiao}...")
        df_temp = get_ine_data_api(codigo, cod_regiao)

        if not df_temp.empty:
            buffer_dados.append(df_temp)

        time.sleep(0.5)

    output_path = os.path.join("..", "..", "dados", "bronze", f"{nome}_censo_ine.csv")
    if buffer_dados:
        df_resultado = pd.concat(buffer_dados, ignore_index=True)
        df_resultado.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"Sucesso! Arquivo salvo em: {output_path}")
    else:
        print(f"[FALHA] Nenhum dado retornado para {nome}.")