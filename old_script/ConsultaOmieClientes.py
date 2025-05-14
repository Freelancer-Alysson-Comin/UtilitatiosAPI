import os
import requests
import json
import pandas as pd

# Defina o URL da API e as credenciais
url = "https://app.omie.com.br/api/v1/geral/clientes/"
app_key = "1827861838803"  # Substitua pelo seu app_key
app_secret = "7d4f891378864eab0f42a0f8ea9243ec"  # Substitua pelo seu app_secret

# Cabeçalhos da requisição
headers = {
    "Content-Type": "application/json"
}

# Função para buscar todos os clientes de forma paginada
def buscar_todos_clientes():
    pagina = 1
    registros_por_pagina = 50
    clientes = []
    
    while True:
        # Corpo da requisição para uma página específica
        data = {
            "call": "ListarClientes",
            "app_key": app_key,
            "app_secret": app_secret,
            "param": [
                {
                    "pagina": pagina,
                    "registros_por_pagina": registros_por_pagina,
                    "apenas_importado_api": "N"
                }
            ]
        }
        
        # Fazendo a requisição POST
        response = requests.post(url, headers=headers, data=json.dumps(data))
        
        # Verificando se a resposta foi bem-sucedida
        if response.status_code == 200:
            resposta_json = response.json()
            
            # Verificar se há clientes na resposta
            if "clientes_cadastro" in resposta_json and resposta_json["clientes_cadastro"]:
                # Adicionar os clientes da página à lista total
                clientes.extend(resposta_json["clientes_cadastro"])
                print(f"Página {pagina} processada com sucesso.")
            else:
                print("Não há mais registros a serem processados.")
                break  # Sai do loop quando não há mais registros
            
        else:
            print(f"Erro {response.status_code}: {response.text}")
            break
        
        # Próxima página
        pagina += 1

    return clientes

# Buscar todos os clientes
clientes = buscar_todos_clientes()

# Verificar se há clientes a serem salvos
if clientes:
    # Criar um DataFrame a partir da lista de clientes
    df = pd.DataFrame(clientes)
    
    # Definir o caminho onde o arquivo será salvo
    caminho_diretorio = r"D:\FlyERP\Clientes\Rede Industrial"
    
    # Verificar se o diretório existe, e se não, criar
    if not os.path.exists(caminho_diretorio):
        os.makedirs(caminho_diretorio)

    # Definir o caminho completo do arquivo
    caminho_arquivo = os.path.join(caminho_diretorio, "todos_clientes_omie.xlsx")
    
    # Salvar em um arquivo Excel no caminho especificado
    df.to_excel(caminho_arquivo, index=False)
    print(f"Dados salvos com sucesso no arquivo '{caminho_arquivo}'.")
else:
    print("Nenhum cliente encontrado.")
