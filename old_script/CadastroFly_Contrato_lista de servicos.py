import csv
import time
import requests
import chardet  # type: ignore
import logging
from collections import defaultdict

# Configuração de logging
logging.basicConfig(filename='app.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# URL do webservice
url = "http://demoacconsultsystem.flyerp.com.br/apis/CadastrarContrato"

# Bearer Token para autenticação
bearer_token = "e0eac02a-16c6-4708-b23c-4cedbf8251c8"

# Cabeçalhos da requisição
headers = {
    "Authorization": f"Bearer {bearer_token}",
    "Content-Type": "application/json"
}

# Caminho dos arquivos
csv_file_path = r"D:/FlyERP/Dev/Automação de cadastros/Pasta de Arquivos/teste_contrato_2_servicos.csv"
erro_csv_file_path = r"D:/FlyERP/Dev/Automação de cadastros/Pasta de Arquivos/registros_falhados_lages.csv"

# Função para converter valores decimais corretamente
def format_decimal(value):
    try:
        return value.replace('.', ',') if isinstance(value, str) else str(value)
    except AttributeError:
        return '0'

# Detectar a codificação do arquivo CSV
with open(csv_file_path, 'rb') as file:
    result = chardet.detect(file.read())
encoding = result['encoding']
logging.info(f"Codificação detectada: {encoding}")

# Dicionário para armazenar contratos agrupados por referência
contratos = defaultdict(lambda: {"dados": {}, "servicos": []})

# Ler o arquivo CSV e agrupar contratos
with open(csv_file_path, mode='r', encoding=encoding) as file:
    reader = csv.DictReader(file, delimiter=';')
    reader.fieldnames = [field.strip() for field in reader.fieldnames]

   
    for row in reader:
        referencia = row['referencia']
        
        # Se ainda não adicionamos os dados do contrato, adicionamos
        if not contratos[referencia]["dados"]:
            contratos[referencia]["dados"] = {
                "cnpj_cpf_cliente": row['cnpj_cpf_cliente'],
                "referencia": referencia,
                "data_primeiro_vencimento": row['data_primeiro_vencimento'],
                "dia_vencimento": row['dia_vencimento'],
                "descricao": row['descricao'],
                "numero": row['numero'],                
                "codigo_vendedor": int(row['codigo_vendedor'].strip()) if row['codigo_vendedor'].strip().isdigit() else 0,
                "data_vigencia_inicial": row['data_vigencia_inicial'],
                "data_vigencia_final": row['data_vigencia_final'],
                "situacao": row['situacao'],
                "recorrencia": row['recorrencia'],
                "tipo_faturamento": row['tipo_faturamento'],
                "indice_reajuste": row['indice_reajuste'],
                "codigo_banco_conta": int(row['codigo_banco_conta']) if row['codigo_banco_conta'] else 0,
                "codigo_forma_pagamento": int(row['codigo_forma_pagamento']) if row['codigo_forma_pagamento'] else 0,
                "codigo_plano_contas": int(row['codigo_plano_contas']) if row['codigo_plano_contas'] else 0,
                "codigo_condicao_pagamento": row['codigo_condicao_pagamento'],
                "observacao_fiscal": row['observacao_fiscal'],
                "observacao_nao_fiscal": row['observacao_nao_fiscal'],
                "data_proximo_reajuste": row['data_proximo_reajuste']
            }
        
        # Adicionar serviço ao contrato
        contratos[referencia]["servicos"].append({
            "referencia": row['servicos.referencia'],
            "quantidade": int(row['servicos.quantidade']) if row['servicos.quantidade'] else 1,
            "valor_unitario": format_decimal(row['servicos.valor_unitario']),
            "complemento": row['servicos.complemento']
        })

# Criar arquivo CSV de erros
with open(erro_csv_file_path, mode='w', newline='', encoding='utf-8-sig') as erro_file:
    erro_writer = csv.writer(erro_file)
    erro_writer.writerow(["referencia", "mensagem_erro", "id"])
    
    # Enviar contratos agrupados para a API
    for referencia, contrato in contratos.items():
        contrato_payload = contrato["dados"]
        contrato_payload["servicos"] = contrato["servicos"]
        
        try:
            response = requests.post(url, headers=headers, json=contrato_payload)
            response.raise_for_status()
            response_data = response.json()
            
            if not response_data.get("Sucesso"):
                erro_writer.writerow([referencia, response_data.get("Mensagem", "Erro"), response_data.get("Id", "N/A")])
        
        except Exception as e:
            logging.error(f"Erro ao enviar contrato {referencia}: {e}")
        
        time.sleep(0.2)
