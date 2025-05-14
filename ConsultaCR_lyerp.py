import requests
import json
import os
import time  # Para adicionar um tempo de espera entre requisições

# URL base da API do FlyERP
base_url = 'http://gonzalez.flyerp.com.br/apis/GetContasAReceber'

# Seu token de autenticação Bearer
auth_token = '24b9f710-6739-4898-9511-6c067caa6f9e'

# Cabeçalhos da requisição com autenticação
headers = {
    'Authorization': f'Bearer {auth_token}',
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
}

# Parâmetros fixos da consulta
payload = {
    "dataEmissaoInicial": "01/01/2025",
    "dataEmissaoFinal": "31/12/2025"
}

# Controle de paginação
inicioRegistros = 0  # Começa em 0
per_page = 500
all_data = []
max_requests = 12  # Limite máximo de requisições para evitar loop infinito

for i in range(max_requests):
    # Monta o JSON da requisição com a paginação correta
    payload["inicioRegistros"] = inicioRegistros
    payload["quantidadeRegistros"] = per_page

    try:
        # Faz a requisição POST
        response = requests.post(base_url, json=payload, headers=headers, allow_redirects=False)

        if response.status_code in [301, 302]:  # Se houver redirecionamento, tenta com GET
            print('A API redirecionou a requisição. Tentando com GET...')
            response = requests.get(base_url, params=payload, headers=headers, allow_redirects=False)

        if response.status_code != 200:
            print(f'Erro na requisição: {response.status_code}')
            print(response.text)
            break

        data = response.json()
        
        if not data:
            print('Nenhum dado retornado.')
            break

        all_data.extend(data)

        print(f'Consulta {i+1}: {inicioRegistros} - {inicioRegistros + per_page} ({len(data)} registros)')

        if len(data) < per_page:  # Se a resposta trouxe menos do que o esperado, terminamos
            print('Todos os dados foram carregados.')
            break

        inicioRegistros += per_page  # Atualiza o offset corretamente
        time.sleep(1)  # Espera 1 segundo entre requisições

    except requests.exceptions.RequestException as e:
        print(f'Erro de conexão: {e}')
        break

print(f'Total de registros coletados: {len(all_data)}')

# Define o caminho do arquivo TXT
caminho_txt = r'D:/FlyERP/Clientes/Gonzalez Alimentos/dados_flyerp.txt'

# Garante que a pasta existe antes de salvar
os.makedirs(os.path.dirname(caminho_txt), exist_ok=True)

# Salva os dados no arquivo TXT
with open(caminho_txt, 'w', encoding='utf-8') as file:
    file.write(json.dumps(all_data, indent=4, ensure_ascii=False))

print(f'Dados salvos em "{caminho_txt}".')
