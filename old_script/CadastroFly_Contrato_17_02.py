import csv
import time
import requests
import chardet  # type: ignore
import logging

# Configuração de logging
logging.basicConfig(filename='app.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# URL do webservice
url = "http://aspsoftwares.flyerp.com.br/apis/CadastrarContrato"

# Bearer Token para autenticação
bearer_token = "6edd8eb7-29ac-46d2-ad7f-849322b3d01a"

# Cabeçalhos da requisição incluindo o Bearer Token
headers = {
    "Authorization": f"Bearer {bearer_token}",
    "Content-Type": "application/json"
}

# Caminho do arquivo CSV original e de erro
csv_file_path = r"D:/FlyERP/Dev/Automação de cadastros/Pasta de Arquivos/Contrato_v2_lages.csv"
erro_csv_file_path = r"D:/FlyERP/Dev/Automação de cadastros/Pasta de Arquivos/registros_falhados_lages.csv"

# Função para converter o valor em decimal formatado
def format_decimal(value):
    try:
        return value.replace('.', ',') if isinstance(value, str) else str(value)
    except AttributeError:
        return '0'  # Retorna '0' em caso de erro

# Detectar a codificação do arquivo CSV
with open(csv_file_path, 'rb') as file:
    result = chardet.detect(file.read())
encoding = result['encoding']
logging.info(f"Codificacao detectada: {encoding}")

# Abrir o arquivo CSV para registros falhados
with open(erro_csv_file_path, mode='w', newline='', encoding='utf-8-sig') as erro_file:
    erro_writer = csv.writer(erro_file)
    erro_writer.writerow(['cnpj_cpf_cliente', 'referencia', 'data_primeiro_vencimento', 'dia_vencimento',
                          'descricao', 'numero', 'codigo_vendedor', 'data_vigencia_inicial', 'data_vigencia_final',
                          'situacao', 'recorrencia', 'tipo_faturamento', 'indice_reajuste', 'codigo_banco_conta',
                          'codigo_forma_pagamento', 'codigo_plano_contas', 'codigo_condicao_pagamento',
                          'observacao_fiscal', 'observacao_nao_fiscal', 'data_proximo_reajuste', 'servicos.referencia',
                          'servicos.quantidade', 'servicos.valor_unitario', 'servicos.complemento', 'Mensagem', 'Id'])

    # Ler arquivo CSV original
    try:
        with open(csv_file_path, mode='r', encoding=encoding) as file:
            reader = csv.DictReader(file, delimiter=';')
            reader.fieldnames = [field.strip() for field in reader.fieldnames]

            for row in reader:
                cnpj_cpf_cliente = row.get('cnpj_cpf_cliente')
                referencia = row.get('referencia')
                data_primeiro_vencimento = row.get('data_primeiro_vencimento')
                dia_vencimento = row.get('dia_vencimento')
                descricao = row.get('descricao')
                numero = row.get('numero')
                codigo_vendedor = row.get('codigo_vendedor')
                data_vigencia_inicial = row.get('data_vigencia_inicial')
                data_vigencia_final = row.get('data_vigencia_final')
                situacao = row.get('situacao')
                recorrencia = row.get('recorrencia')
                tipo_faturamento = row.get('tipo_faturamento')
                indice_reajuste = row.get('indice_reajuste')
                codigo_banco_conta = row.get('codigo_banco_conta')
                codigo_forma_pagamento = row.get('codigo_forma_pagamento')
                codigo_plano_contas = row.get('codigo_plano_contas')
                codigo_condicao_pagamento = row.get('codigo_condicao_pagamento')
                observacao_fiscal = row.get('observacao_fiscal')
                observacao_nao_fiscal = row.get('observacao_nao_fiscal')
                data_proximo_reajuste = row.get('data_proximo_reajuste')  # Novo campo
                servicos_referencia = row.get('servicos.referencia')
                servicos_quantidade = row.get('servicos.quantidade')
                servicos_valor_unitario = format_decimal(row.get('servicos.valor_unitario', '0'))
                servicos_complemento = row.get('servicos.complemento')

                contrato_a_receber = {
                    "cnpj_cpf_cliente": cnpj_cpf_cliente,
                    "referencia": referencia,
                    "data_primeiro_vencimento": data_primeiro_vencimento,
                    "dia_vencimento": dia_vencimento,
                    "descricao": descricao,
                    "numero": numero,
                    "codigo_vendedor": int(codigo_vendedor) if codigo_vendedor else 0,
                    "data_vigencia_inicial": data_vigencia_inicial,
                    "data_vigencia_final": data_vigencia_final,
                    "situacao": situacao,
                    "recorrencia": recorrencia,
                    "tipo_faturamento": tipo_faturamento,
                    "indice_reajuste": indice_reajuste,
                    "codigo_banco_conta": int(codigo_banco_conta) if codigo_banco_conta else 0,
                    "codigo_forma_pagamento": int(codigo_forma_pagamento) if codigo_forma_pagamento else 0,
                    "codigo_plano_contas": int(codigo_plano_contas) if codigo_plano_contas else 0,
                    "codigo_condicao_pagamento": codigo_condicao_pagamento,
                    "observacao_fiscal": observacao_fiscal,
                    "observacao_nao_fiscal": observacao_nao_fiscal,
                    "data_proximo_reajuste": data_proximo_reajuste,
                    "servicos": [
                        {
                            "referencia": servicos_referencia,
                            "quantidade": int(servicos_quantidade) if servicos_quantidade else 1,
                            "valor_unitario": servicos_valor_unitario,
                            "complemento": servicos_complemento
                        }
                    ]
                }
                
                response = requests.post(url, headers=headers, json=contrato_a_receber)
                response.raise_for_status()
                response_data = response.json()

                if not response_data.get("Sucesso"):
                    erro_writer.writerow(list(contrato_a_receber.values()) + [response_data.get("Mensagem", "Erro"), response_data.get("Id", "N/A")])
                
                time.sleep(0.2)
    except Exception as e:
        logging.error(f"Erro inesperado: {e}")
