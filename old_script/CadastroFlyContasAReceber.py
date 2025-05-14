import csv
import time
import requests
import chardet # type: ignore
import logging

# Configuração de logging
logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# URL do webservice (adapte para seu ambiente)
url = "http://dcembalagens.flyerp.com.br/APIs/CadastrarContasAReceber"

# Bearer Token para autenticação
bearer_token = "98e002ab-fd68-4512-9377-0c04e4ef40a4"

# Cabeçalhos da requisição incluindo o Bearer Token
headers = {
    "Authorization": f"Bearer {bearer_token}",
    "Content-Type": "application/json"
}

# Caminho do arquivo CSV original e de erro
csv_file_path = r"D:/FlyERP/Dev/Automação de cadastros/Pasta de Arquivos/importtar_Contas_receber_dc_erros.csv"
erro_csv_file_path = r"D:/FlyERP/Dev/Automação de cadastros/Pasta de Arquivos/Contas_receber_dc_erros.csv"

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
    erro_writer.writerow(['codigo_pessoa', 'cpfcnpj_pessoa', 'codigo_banco', 'codigo_forma_pagamento',
                          'data_emissao', 'data_vencimento', 'valor', 'observacao', 'id_importacao',
                          'parcela', 'numero_titulo', 'numero_documento', 'valor_pago',
                          'data_ultimo_pagamento', 'codigo_plano_contas', 'valor_juros_pago',
                          'valor_desconto_pago', 'Mensagem', 'Id'])

    # Ler arquivo CSV original
    try:
        with open(csv_file_path, mode='r', encoding=encoding) as file:
            reader = csv.DictReader(file, delimiter=';')
            reader.fieldnames = [field.strip() for field in reader.fieldnames]

            for row in reader:
                codigo_pessoa = row.get('codigo_pessoa')
                cpfcnpj_pessoa = row.get('cpfcnpj_pessoa')
                codigo_banco = row.get('codigo_banco')
                codigo_forma_pagamento = row.get('codigo_forma_pagamento')
                data_emissao = row.get('data_emissao')
                data_vencimento = row.get('data_vencimento')
                valor = format_decimal(row.get('valor', '0'))
                observacao = row.get('observacao')
                id_importacao = row.get('id_importacao')
                parcela = row.get('parcela')
                numero_titulo = row.get('numero_titulo')
                numero_documento = row.get('numero_documento')
                valor_pago = format_decimal(row.get('valor_pago', '0'))
                data_ultimo_pagamento = row.get('data_ultimo_pagamento')
                codigo_plano_contas = row.get('codigo_plano_contas')
                valor_juros_pago = format_decimal(row.get('valor_juros_pago', '0'))
                valor_desconto_pago = format_decimal(row.get('valor_desconto_pago', '0'))
                
                if cpfcnpj_pessoa is None:
                    logging.warning("Coluna 'cpfcnpj_pessoa' não encontrada no CSV. Ignorando registro.")
                    continue
                
                try:
                    contas_a_receber = {
                        "codigo_pessoa": codigo_pessoa,
                        "cpfcnpj_pessoa": cpfcnpj_pessoa,
                        "codigo_banco": int(codigo_banco) if codigo_banco else 0,
                        "codigo_forma_pagamento": int(codigo_forma_pagamento) if codigo_forma_pagamento else 0,
                        "data_emissao": data_emissao,
                        "data_vencimento": data_vencimento,
                        "valor": valor,
                        "observacao": observacao,
                        "id_importacao": id_importacao,
                        "parcela": int(parcela) if parcela else 1,
                        "numero_titulo": int(numero_titulo) if numero_titulo else 0,
                        "numero_documento": int(numero_documento) if numero_documento else 0,
                        "valor_pago": valor_pago,
                        "data_ultimo_pagamento": data_ultimo_pagamento,
                        "codigo_plano_contas": int(codigo_plano_contas) if codigo_plano_contas else 0,
                        "valor_juros_pago": valor_juros_pago,
                        "valor_desconto_pago": valor_desconto_pago
                    }
                except ValueError as ve:
                    logging.error(f"Erro de conversão de valor no registro: {row}. Erro: {ve}")
                    erro_writer.writerow([codigo_pessoa, cpfcnpj_pessoa, codigo_banco, codigo_forma_pagamento,
                                         data_emissao, data_vencimento, valor, observacao, id_importacao,
                                         parcela, numero_titulo, numero_documento, valor_pago,
                                         data_ultimo_pagamento, codigo_plano_contas, valor_juros_pago,
                                         valor_desconto_pago, f"Erro de conversão: {ve}", "N/A"])
                    continue
                
                try:
                    response = requests.post(url, headers=headers, json=contas_a_receber)
                    response.raise_for_status()

                    response_data = response.json()

                    if response_data.get("Sucesso"):
                        logging.info(f"Sucesso: {response_data['Mensagem']}, ID: {response_data['Id']}")
                    else:
                        logging.error(f"Erro: {response_data['Mensagem']}, ID: {response_data.get('Id', 'N/A')}")
                        erro_writer.writerow([codigo_pessoa, cpfcnpj_pessoa, codigo_banco, codigo_forma_pagamento,
                                             data_emissao, data_vencimento, valor, observacao, id_importacao,
                                             parcela, numero_titulo, numero_documento, valor_pago,
                                             data_ultimo_pagamento, codigo_plano_contas, valor_juros_pago,
                                             valor_desconto_pago, response_data.get("Mensagem", ""),
                                             response_data.get("Id", "N/A")])

                except requests.exceptions.HTTPError as errh:
                    logging.error(f"Erro HTTP: {errh} - Registro {numero_documento} não foi enviado.")
                    erro_writer.writerow([codigo_pessoa, cpfcnpj_pessoa, codigo_banco, codigo_forma_pagamento,
                                         data_emissao, data_vencimento, valor, observacao, id_importacao,
                                         parcela, numero_titulo, numero_documento, valor_pago,
                                         data_ultimo_pagamento, codigo_plano_contas, valor_juros_pago,
                                         valor_desconto_pago, f"Erro HTTP: {errh}", "N/A"])
                except requests.exceptions.ConnectionError as errc:
                    logging.error(f"Erro de Conexão: {errc}")
                    erro_writer.writerow([codigo_pessoa, cpfcnpj_pessoa, codigo_banco, codigo_forma_pagamento,
                                         data_emissao, data_vencimento, valor, observacao, id_importacao,
                                         parcela, numero_titulo, numero_documento, valor_pago,
                                         data_ultimo_pagamento, codigo_plano_contas, valor_juros_pago,
                                         valor_desconto_pago, f"Erro de Conexão: {errc}", "N/A"])
                except requests.exceptions.Timeout as errt:
                    logging.error(f"Timeout: {errt}")
                    erro_writer.writerow([codigo_pessoa, cpfcnpj_pessoa, codigo_banco, codigo_forma_pagamento,
                                         data_emissao, data_vencimento, valor, observacao, id_importacao,
                                         parcela, numero_titulo, numero_documento, valor_pago,
                                         data_ultimo_pagamento, codigo_plano_contas, valor_juros_pago,
                                         valor_desconto_pago, f"Timeout: {errt}", "N/A"])
                except requests.exceptions.RequestException as err:
                    logging.error(f"Erro: {err}")
                    erro_writer.writerow([codigo_pessoa, cpfcnpj_pessoa, codigo_banco, codigo_forma_pagamento,
                                         data_emissao, data_vencimento, valor, observacao, id_importacao,
                                         parcela, numero_titulo, numero_documento, valor_pago,
                                         data_ultimo_pagamento, codigo_plano_contas, valor_juros_pago,
                                         valor_desconto_pago, f"Erro: {err}", "N/A"])
    finally:
                    print("Bloco 'finally' executado.")
    time.sleep(4)
