import os
import requests
import json
import pandas as pd

# Defina o URL da API e as credenciais
url = "https://app.omie.com.br/api/v1/geral/categorias/"
app_key = "998306335026"  # Substitua pelo seu app_key
app_secret = "6df61b0d072c891cde00cefbf99fae01"  # Substitua pelo seu app_secret

# Cabeçalhos da requisição
headers = {
    "Content-Type": "application/json"
}

# Função para buscar todas as categorias de forma paginada
def buscar_todas_categorias():
    pagina = 1
    registros_por_pagina = 50
    categorias = []
    
    while True:
        # Corpo da requisição para uma página específica
        data = {
            "call": "ListarCategorias",
            "app_key": app_key,
            "app_secret": app_secret,
            "param": [
                {
                    "pagina": pagina,
                    "registros_por_pagina": registros_por_pagina
                }
            ]
        }
        
        # Fazendo a requisição POST
        response = requests.post(url, headers=headers, data=json.dumps(data))
        
        # Verificando se a resposta foi bem-sucedida
        if response.status_code == 200:
            resposta_json = response.json()
            
            # Verificar se há categorias na resposta
            if "categoria_cadastro" in resposta_json and resposta_json["categoria_cadastro"]:
                # Adicionar as categorias da página à lista total
                categorias.extend(resposta_json["categoria_cadastro"])
                print(f"Página {pagina} processada com sucesso.")
            else:
                print("Não há mais registros a serem processados.")
                break  # Sai do loop quando não há mais registros
            
        else:
            print(f"Erro {response.status_code}: {response.text}")
            break
        
        # Próxima página
        pagina += 1

    return categorias

# Buscar todas as categorias
categorias = buscar_todas_categorias()

# Verificar se há categorias a serem salvas
if categorias:
    # Normalizar e descompactar JSON aninhado para um DataFrame
    df = pd.json_normalize(categorias)
    
    # Definir o caminho onde o arquivo será salvo
    caminho_diretorio = r"D:\FlyERP\Clientes\Rede Industrial"
    
    # Verificar se o diretório existe, e se não, criar
    if not os.path.exists(caminho_diretorio):
        os.makedirs(caminho_diretorio)

    # Definir o caminho completo do arquivo
    caminho_arquivo = os.path.join(caminho_diretorio, "todas_categorias_omie.xlsx")
    
    # Salvar em um arquivo Excel no caminho especificado
    df.to_excel(caminho_arquivo, index=False)
    print(f"Dados salvos com sucesso no arquivo '{caminho_arquivo}'.")
else:
    print("Nenhuma categoria encontrada.")
