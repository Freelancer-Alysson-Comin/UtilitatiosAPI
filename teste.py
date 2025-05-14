import os
import requests
import json
import pandas as pd

# Defina o URL da API e as credenciais
url = "https://app.omie.com.br/api/v1/servicos/contrato/"
app_key = "998306335026"  # Substitua pelo seu app_key
app_secret = "6df61b0d072c891cde00cefbf99fae01"  # Substitua pelo seu app_secret

# Cabeçalhos da requisição
headers = {
    "Content-Type": "application/json"
}

def buscar_todos_contratos():
    """
    Busca todos os contratos de forma paginada da API Omie.
    
    Returns:
        List: Lista de contratos retornados pela API.
    """
    pagina = 1
    registros_por_pagina = 50
    contratos = []
    
    while True:
        # Corpo da requisição para uma página específica
        data = {
            "call": "ListarContratos",
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
        
        try:
            # Fazendo a requisição POST
            response = requests.post(url, headers=headers, data=json.dumps(data))
            response.raise_for_status()  # Levanta um erro para códigos de status HTTP de erro
            
            resposta_json = response.json()
            
            # Imprimir a resposta completa para inspecionar a estrutura
            print(f"Resposta JSON da página {pagina}:")
            print(json.dumps(resposta_json, indent=4))  # Formatar a resposta para melhor visualização
            
            # Verificar se a chave 'contratoCadastro' existe na resposta
            if "contratoCadastro" in resposta_json:
                contratos_pagina = resposta_json["contratoCadastro"]
                if contratos_pagina:
                    contratos.extend(contratos_pagina)
                    print(f"Página {pagina} processada com sucesso.")
                else:
                    print(f"Nenhum contrato encontrado nesta página {pagina}.")
                    break  # Sai do loop quando não há mais registros
            else:
                print("Chave 'contratoCadastro' não encontrada ou estrutura de resposta inesperada.")
                break
            
        except requests.exceptions.RequestException as e:
            print(f"Erro de requisição: {e}")
            break
        
        # Próxima página
        pagina += 1

    return contratos

# Buscar todos os contratos
contratos = buscar_todos_contratos()

# Verificar se há contratos a serem salvos
if contratos:
    # Criar um DataFrame a partir da lista de contratos
    contratos_expandido = []

    for contrato in contratos:
        # Extraindo os dados relevantes do contrato
        contrato_dados = {
            "cCodSit": contrato["cabecalho"].get("cCodSit"),
            "cNumCtr": contrato["cabecalho"].get("cNumCtr"),
            "dVigInicial": contrato["cabecalho"].get("dVigInicial"),
            "dVigFinal": contrato["cabecalho"].get("dVigFinal"),
            "nValTotMes": contrato["cabecalho"].get("nValTotMes"),
            "itemDescricao": contrato["itensContrato"][0]["itemDescrServ"].get("descrCompleta") if contrato["itensContrato"] else None,
            "valorTotal": contrato["itensContrato"][0]["itemCabecalho"].get("valorTotal") if contrato["itensContrato"] else None
        }
        contratos_expandido.append(contrato_dados)

    df = pd.DataFrame(contratos_expandido)

    # Definir o caminho onde você deseja salvar o arquivo CSV
    caminho_csv = "D:\\FlyERP\\Clientes\\rede_industrial_contratos.csv"
    
    # Salvar o DataFrame em um arquivo CSV
    df.to_csv(caminho_csv, index=False, encoding='utf-8-sig')
    print(f"Contratos salvos em {caminho_csv}.")
else:
    print("Nenhum contrato encontrado.")
