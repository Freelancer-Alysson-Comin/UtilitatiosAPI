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
        contrato_dados = {}
        
        # Extraindo todos os campos do cabeçalho
        for key, value in contrato["cabecalho"].items():
            contrato_dados[f"cabecalho_{key}"] = value
        
        # Extraindo todos os campos dos departamentos
        for idx, dep in enumerate(contrato.get("departamentos", [])):
            for key, value in dep.items():
                contrato_dados[f"departamento_{idx}_{key}"] = value
        
        # Extraindo todos os campos da informação adicional
        for key, value in contrato.get("infAdic", {}).items():
            contrato_dados[f"infAdic_{key}"] = value
        
        # Extraindo todos os campos de itens do contrato
        for idx, item in enumerate(contrato.get("itensContrato", [])):
            for key, value in item["itemCabecalho"].items():
                contrato_dados[f"item_{idx}_cabecalho_{key}"] = value
            for key, value in item["itemDescrServ"].items():
                contrato_dados[f"item_{idx}_descricao_{key}"] = value
            for key, value in item["itemImpostos"].items():
                contrato_dados[f"item_{idx}_impostos_{key}"] = value
            for key, value in item["itemLeiTranspImp"].items():
                contrato_dados[f"item_{idx}_lei_transp_imp_{key}"] = value
        
        contratos_expandido.append(contrato_dados)

    # Criar o DataFrame a partir da lista de contratos
    df = pd.DataFrame(contratos_expandido)

    # Definir o caminho onde você deseja salvar o arquivo CSV
    caminho_csv = "D:\\FlyERP\\Clientes\\rede_industrial_contratos.csv"

    # Salvar o DataFrame em um arquivo CSV
    try:
        df.to_csv(caminho_csv, index=False, encoding='utf-8-sig')
        print(f"Contratos salvos em {caminho_csv}.")
    except PermissionError as e:
        print(f"Erro ao salvar o arquivo: {e}. Verifique se o arquivo está aberto ou se você tem permissão para escrever no diretório.")
else:
    print("Nenhum contrato encontrado.")
