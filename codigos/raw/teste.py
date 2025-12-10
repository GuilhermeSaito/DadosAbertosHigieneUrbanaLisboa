import requests

def procurar_indicadores(termo_busca):
    url = "https://www.ine.pt/ine/json_indicador/pindica.jsp"
    params = {
        "op": "1",           # Operação 1 = Pesquisa
        "lang": "PT",
        "search": termo_busca
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        print(f"--- Resultados para '{termo_busca}' ---")
        if not data:
            print("Nenhum indicador encontrado.")
            return

        for ind in data:
            # Filtra apenas os que parecem ser do Censos 2021
            nome = ind.get('IndicadorNome', '')
            codigo = ind.get('IndicadorCod', '')
            
            # Mostra o resultado
            print(f"[{codigo}] {nome}")
            
    except Exception as e:
        print(f"Erro na busca: {e}")

# =======================================================
# EXEMPLOS DE BUSCA (Rode isto para descobrir os códigos)
# =======================================================
print("Procurando códigos do Censos...\n")

# 1. Para substituir a tabela de População/Famílias
procurar_indicadores("Estado civil")

# 2. Para Edifícios (Q201)
print("\n")
procurar_indicadores("Nível de escolaridade mais elevado completo")

# 3. Para Escolaridade (Q603)
print("\n")
procurar_indicadores("Edifícios clássicos")