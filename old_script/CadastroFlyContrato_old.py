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
bearer_token = "f03d7a9c-9733-482c-9f65-a22ddc109260"

# Cabeçalhos da requisição incluindo o Bearer Token
headers = {
    "Authorization": f"Bearer {bearer_token}",
    "Content-Type": "application/json"
}

# Caminho do arquivo CSV original e de erro
csv_file_path = r"D:/FlyERP/Dev/Automação de cadastros/Pasta de Arquivos/Contrato_v1_brusque.csv"
erro_csv_file_path = r"D:/FlyERP/Dev/Automação de cadastros/Pasta de Arquivos/registros_falhados_brusque.csv"

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
logging.info(f"Codificação detectada: {encoding}")

# Abrir o arquivo CSV para registros falhados
with open(erro_csv_file_path, mode='w', newline='', encoding='utf-8-sig') as erro_file:
    erro_writer = csv.writer(erro_file)
    erro_writer.writerow(['cnpj_cpf_cliente', 'referencia', 'data_primeiro_vencimento', 'dia_vencimento',
                          'descricao', 'numero', 'codigo_vendedor', 'data_vigencia_inicial', 'data_vigencia_final',
                          'situacao', 'recorrencia', 'tipo_faturamento', 'indice_reajuste', 'codigo_banco_conta',
                          'codigo_forma_pagamento', 'codigo_plano_contas', 'codigo_condicao_pagamento',
                          'observacao_fiscal', 'observacao_nao_fiscal', 'servicos.referencia', 'servicos.quantidade',
                          'servicos.valor_unitario', 'servicos.complemento', 'Mensagem', 'Id'])

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
                servicos_referencia = row.get('servicos.referencia')
                servicos_quantidade = row.get('servicos.quantidade')
                servicos_valor_unitario = format_decimal(row.get('servicos.valor_unitario', '0'))
                servicos_complemento = row.get('servicos.complemento')  # Novo campo

                if cnpj_cpf_cliente is None:
                    logging.warning("Coluna 'cnpj_cpf_cliente' não encontrada no CSV. Ignorando registro.")
                    continue

                try:
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
                        "servicos": [
                            {
                                "referencia": servicos_referencia,
                                "quantidade": int(servicos_quantidade) if servicos_quantidade else 1,
                                "valor_unitario": servicos_valor_unitario,
                                "complemento": servicos_complemento  # Novo campo adicionado
                            }
                        ]
                    }
                except ValueError as ve:
                    logging.error(f"Erro de conversão de valor no registro: {row}. Erro: {ve}")
                    erro_writer.writerow([cnpj_cpf_cliente, referencia, data_primeiro_vencimento, dia_vencimento,
                                         descricao, numero, codigo_vendedor, data_vigencia_inicial, data_vigencia_final,
                                         situacao, recorrencia, tipo_faturamento, indice_reajuste, codigo_banco_conta,
                                         codigo_forma_pagamento, codigo_plano_contas, codigo_condicao_pagamento,
                                         observacao_fiscal, observacao_nao_fiscal, servicos_referencia, servicos_quantidade,
                                         servicos_valor_unitario, servicos_complemento, f"Erro de conversão: {ve}", "N/A"])
                    continue

                try:
                    response = requests.post(url, headers=headers, json=contrato_a_receber)
                    response.raise_for_status()

                    response_data = response.json()

                    if response_data.get("Sucesso"):
                        logging.info(f"Sucesso: {response_data.get('Mensagem', 'Mensagem não disponível')}, ID: {response_data.get('Id', 'N/A')}")
                    else:
                        logging.error(f"Erro: {response_data.get('Mensagem', 'Mensagem não disponível')}, ID: {response_data.get('Id', 'N/A')}")
                        erro_writer.writerow([cnpj_cpf_cliente, referencia, data_primeiro_vencimento, dia_vencimento,
                                             descricao, numero, codigo_vendedor, data_vigencia_inicial, data_vigencia_final,
                                             situacao, recorrencia, tipo_faturamento, indice_reajuste, codigo_banco_conta,
                                             codigo_forma_pagamento, codigo_plano_contas, codigo_condicao_pagamento,
                                             observacao_fiscal, observacao_nao_fiscal, servicos_referencia, servicos_quantidade,
                                             servicos_valor_unitario, servicos_complemento, response_data.get("Mensagem", "Mensagem não disponível"),
                                             response_data.get("Id", "N/A")])

                except requests.exceptions.HTTPError as errh:
                    logging.error(f"Erro HTTP: {errh} - Registro {numero} não foi enviado.")
                    erro_writer.writerow([cnpj_cpf_cliente, referencia, data_primeiro_vencimento, dia_vencimento,
                                         descricao, numero, codigo_vendedor, data_vigencia_inicial, data_vigencia_final,
                                         situacao, recorrencia, tipo_faturamento, indice_reajuste, codigo_banco_conta,
                                         codigo_forma_pagamento, codigo_plano_contas, codigo_condicao_pagamento,
                                         observacao_fiscal, observacao_nao_fiscal, servicos_referencia, servicos_quantidade,
                                         servicos_valor_unitario, servicos_complemento, f"Erro HTTP: {errh}", "N/A"])
                except requests.exceptions.ConnectionError as errc:
                    logging.error(f"Erro de Conexão: {errc}")
                    erro_writer.writerow([cnpj_cpf_cliente, referencia, data_primeiro_vencimento, dia_vencimento,
                                         descricao, numero, codigo_vendedor, data_vigencia_inicial, data_vigencia_final,
                                         situacao, recorrencia, tipo_faturamento, indice_reajuste, codigo_banco_conta,
                                         codigo_forma_pagamento, codigo_plano_contas, codigo_condicao_pagamento,
                                         observacao_fiscal, observacao_nao_fiscal, servicos_referencia, servicos_quantidade,
                                         servicos_valor_unitario, servicos_complemento, f"Erro de Conexão: {errc}", "N/A"])
                except requests.exceptions.Timeout as errt:
                    logging.error(f"Erro de Timeout: {errt}")
                    erro_writer.writerow([cnpj_cpf_cliente, referencia, data_primeiro_vencimento, dia_vencimento,
                                         descricao, numero, codigo_vendedor, data_vigencia_inicial, data_vigencia_final,
                                         situacao, recorrencia, tipo_faturamento, indice_reajuste, codigo_banco_conta,
                                         codigo_forma_pagamento, codigo_plano_contas, codigo_condicao_pagamento,
                                         observacao_fiscal, observacao_nao_fiscal, servicos_referencia, servicos_quantidade,
                                         servicos_valor_unitario, servicos_complemento, f"Erro de Timeout: {errt}", "N/A"])
                except requests.exceptions.RequestException as err:
                    logging.error(f"Erro de Requisição: {err}")
                    erro_writer.writerow([cnpj_cpf_cliente, referencia, data_primeiro_vencimento, dia_vencimento,
                                         descricao, numero, codigo_vendedor, data_vigencia_inicial, data_vigencia_final,
                                         situacao, recorrencia, tipo_faturamento, indice_reajuste, codigo_banco_conta,
                                         codigo_forma_pagamento, codigo_plano_contas, codigo_condicao_pagamento,
                                         observacao_fiscal, observacao_nao_fiscal, servicos_referencia, servicos_quantidade,
                                         servicos_valor_unitario, servicos_complemento, f"Erro de Requisição: {err}", "N/A"])

                # Adiciona um tempo de espera para não sobrecarregar o servidor
                time.sleep(0.2)

    except FileNotFoundError as e:
        logging.error(f"Erro ao tentar abrir o arquivo CSV: {e}")
    except Exception as e:
        logging.error(f"Erro inesperado: {e}")
