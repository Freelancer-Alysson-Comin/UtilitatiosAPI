import csv
import time
import requests
import chardet  # type: ignore
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# URL do webservice
url = "https://exactacontabil.flyerp.com.br/apis/CadastrarContrato"

# Bearer Token para autenticação
bearer_token = "1c3c4bcc-d7ac-4e90-8957-833b3d9fe333"

# Cabeçalhos da requisição incluindo o Bearer Token
headers = {
    "Authorization": f"Bearer {bearer_token}",
    "Content-Type": "application/json"
}

# Caminho do arquivo CSV original e de erro
csv_file_path = r"D:/FlyERP/Dev/Automação de cadastros/Pasta de Arquivos/CONTRATOS SEM DUPLICACAO EXACTA.csv"
erro_csv_file_path = r"D:/FlyERP/Dev/Automação de cadastros/Pasta de Arquivos/registros_falhados_TESTE_EXACTA.csv"

# Função para converter o valor em decimal formatado
def format_decimal(value):
    try:
        return value.replace('.', ',') if isinstance(value, str) else str(value)
    except AttributeError:
        return '0'  # Retorna '0' em caso de erro

# Detectar a codificação do arquivo CSV
with open(csv_file_path, 'rb') as file:
    result = chardet.detect(file.read())
encoding = result.get('encoding', 'utf-8-sig')
logging.info(f"Codificação detectada: {encoding}")

# Abrir o arquivo CSV para registros falhados
with open(erro_csv_file_path, mode='w', newline='', encoding='utf-8-sig') as erro_file:
    erro_writer = csv.writer(erro_file)
    erro_writer.writerow(['cnpj_cpf_cliente', 'referencia', 'data_primeiro_vencimento', 'dia_vencimento',
                          'descricao', 'numero', 'codigo_vendedor', 'data_vigencia_inicial', 'data_vigencia_final',
                          'situacao', 'recorrencia', 'tipo_faturamento', 'indice_reajuste', 'codigo_banco_conta',
                          'codigo_forma_pagamento', 'codigo_plano_contas', 'codigo_condicao_pagamento',
                          'observacao_fiscal', 'observacao_nao_fiscal', 'data_proximo_reajuste', 'servicos.referencia',
                          'servicos.quantidade', 'servicos.valor_unitario', 'servicos.complemento',
                          'tipo_desconto_boleto_ate_vencimento', 'valor_desconto_boleto_ate_vencimento', 'Mensagem', 'Id'])

# Ler arquivo CSV original
try:
    with open(csv_file_path, mode='r', encoding=encoding) as file:
        reader = csv.DictReader(file, delimiter=';')
        reader.fieldnames = [field.strip() for field in reader.fieldnames]

        for row in reader:
            logging.info(f"Linha lida: {row}")

            # Montagem do payload
            contrato_a_receber = {
                "cnpj_cpf_cliente": row.get('cnpj_cpf_cliente', ''),
                "referencia": row.get('referencia', ''),
                "data_primeiro_vencimento": row.get('data_primeiro_vencimento', ''),
                "dia_vencimento": row.get('dia_vencimento', ''),
                "descricao": row.get('descricao', ''),
                "numero": row.get('numero', ''),
                "codigo_vendedor": int(row.get('codigo_vendedor', 0)),
                "data_vigencia_inicial": row.get('data_vigencia_inicial', ''),
                "data_vigencia_final": row.get('data_vigencia_final', ''),
                "situacao": row.get('situacao', ''),
                "recorrencia": row.get('recorrencia', ''),
                "tipo_faturamento": row.get('tipo_faturamento', ''),
                "indice_reajuste": row.get('indice_reajuste', ''),
                "codigo_banco_conta": int(row.get('codigo_banco_conta', 0)),
                "codigo_forma_pagamento": int(row.get('codigo_forma_pagamento', 0)),
                "codigo_plano_contas": int(row.get('codigo_plano_contas', 0)),
                "codigo_condicao_pagamento": row.get('codigo_condicao_pagamento', ''),
                "observacao_fiscal": row.get('observacao_fiscal', ''),
                "observacao_nao_fiscal": row.get('observacao_nao_fiscal', ''),
                "data_proximo_reajuste": row.get('data_proximo_reajuste', ''),
                "tipo_desconto_boleto_ate_vencimento": row.get('tipo_desconto_boleto_ate_vencimento', '0'),
                "valor_desconto_boleto_ate_vencimento": format_decimal(row.get('valor_desconto_boleto_ate_vencimento', '0')),
                "servicos": [
                    {
                        "referencia": row.get('servicos.referencia', ''),
                        "quantidade": int(row.get('servicos.quantidade', 1)),
                        "valor_unitario": format_decimal(row.get('servicos.valor_unitario', '0')),
                        "complemento": row.get('servicos.complemento', '')
                    }
                ]
            }
            logging.info(f"Payload montado: {contrato_a_receber}")

            # Envio do payload
            response = requests.post(url, headers=headers, json=contrato_a_receber)
            response_data = response.json()
            logging.info(f"Resposta da API: {response_data}")

            if response.status_code != 200 or not response_data.get("Sucesso", False):
                erro_writer.writerow(list(contrato_a_receber.values()) + [response_data.get("Mensagem", "Erro"), response_data.get("Id", "N/A")])
                logging.error(f"Erro ao cadastrar contrato {row.get('referencia')}: {response_data.get('Mensagem', 'Erro desconhecido')}")
            else:
                logging.info(f"Contrato {row.get('referencia')} cadastrado com sucesso!")

            time.sleep(0.2)

except Exception as e:
    logging.error(f"Erro inesperado: {e}")
