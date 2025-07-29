import requests
import pandas as pd
import urllib3
import os
import math

from sqlalchemy import create_engine
from datetime import datetime, timedelta
from supabase import create_client, Client
from io import BytesIO
from dotenv import load_dotenv
from zoneinfo import ZoneInfo
from supabase import create_client
import pandas as pd
import numpy as np

# =========================================
# Carregamento das credenciais do ambiente
# =========================================
load_dotenv()
# Acesso SQL
PASSOWORD = os.getenv("SQL_PASSWORD")

# Acesso PI Web API
TOKEN_PI_API = os.getenv("TOKEN_PI_API")
URL_API_PI = os.getenv("URL_API_PI")
ARQUIVO_PI_WEB_API = os.getenv("ARQUIVO_PI_WEB_API")

#Acesso Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

#Acesso Onedrive
LINK_ARQUIVO_DIA_ATUAL = os.getenv("LINK_ARQUIVO_DIA_ATUAL")
LINK_ARQUIVO_HISTORICO = os.getenv("LINK_ARQUIVO_HISTORICO")

# =======================================
# Consultando o banco de dados SQL da F2M
# =======================================

# função para coleta dos dados
def consulta_dados_transporte():
    server = 'sql-f2m-databases-2-prod.miningcontrol.cloud'
    database = 'mining_control_ama'
    username = 'aura_almas_bi'
    password = PASSOWORD

    # String de conexão compatível com SQLAlchemy
    conn_str = (
        f"mssql+pyodbc://{username}:{password}@{server}/{database}"
        "?driver=ODBC+Driver+18+for+SQL+Server"
        "&Encrypt=yes"
        "&TrustServerCertificate=yes"
    )
    # Criação da engine SQLAlchemy
    engine = create_engine(conn_str)

#Coleta os dados dos ultimos 32 dias para exibição dos dados acumulados do mês
    end_time = datetime.now()
    start_time = (end_time - timedelta(days=32)).replace(hour=0, minute=0, second=0, microsecond=0)
    start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_time.strftime('%Y-%m-%d %H:%M:%S')

    colunas_selecionadas = ['datetime_end','origin','destination_subarea','exception_type','material_group', 'calculated_mass','material']
    colunas_sql = ', '.join(colunas_selecionadas)

    query = f"""
    SELECT {colunas_sql}
    FROM [dbo].[dw_transport_report]
    WHERE [datetime_end] >= '{start_str}' AND [datetime_end] <= '{end_str}'
    """

    try:
        df = pd.read_sql(query, engine)
    except Exception as e:
        print("Erro ao executar a consulta SQL com SQLAlchemy:", e)
        df = pd.DataFrame()
    return df

df_dados_mina = consulta_dados_transporte()

#===========================================================
# Coleta de dados do PI Web API - Metodo de Agregação Total
#===========================================================

# Desativa o aviso de conexão insegura
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Função para arredondar segundos para múltiplos de 0 ou 5
def round_to_last_0_or_5_seconds(dt):
    seconds = dt.second
    if seconds % 10 < 5:
        rounded_seconds = seconds - (seconds % 10)
    else:
        rounded_seconds = seconds - (seconds % 10) + 5
    rounded = dt - timedelta(seconds=seconds - rounded_seconds, microseconds=dt.microsecond)
    return rounded

# Função para consultar valores agregados (Total por hora) de um WebID
def fetch_data_for_webid(webid, start_time, end_time, interval):
    base_url = URL_API_PI
    url = f"{base_url}{webid}/summary"

    start_time = round_to_last_0_or_5_seconds(start_time)
    end_time = round_to_last_0_or_5_seconds(end_time)

    params = {
        "startTime": start_time.isoformat(),
        "endTime": end_time.isoformat(),
        "summaryType": "Total",
        "summaryDuration": interval,  # Ex: "1h"
        "calculationBasis": "TimeWeighted",
        "timeType": "Auto"
    }

    response = requests.get(url, headers=headers, params=params, verify=False)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erro ao obter dados para WebID {webid}: {response.status_code} - {response.text}")
        return None

# Cabeçalhos da requisição
headers = {
    "Accept": "application/json",
    "Authorization": TOKEN_PI_API
}

# Caminho do Excel com WebIDs e Apelidos
file_path = ARQUIVO_PI_WEB_API
piwebapi_data = pd.read_excel(file_path)

# Lista de IDs desejados
ids = [11]  # Substitua conforme necessário
webids = piwebapi_data[piwebapi_data['Id'].isin(ids)]['WebId'].tolist()

# Datas de início e fim
end_date = datetime.today().date() + timedelta(days=1)
start_date = end_date - timedelta(days=33)

start_time = datetime.fromisoformat(start_date.strftime('%Y-%m-%d'))
end_time = datetime.fromisoformat(end_date.strftime('%Y-%m-%d'))

interval = "1h"  # Intervalo de agregação

# DataFrame final
df_totalizador = pd.DataFrame()
webids_com_erro = []

# Coleta dos dados
for webid in webids:
    try:
        data = fetch_data_for_webid(webid, start_time, end_time, interval)
        if data:
            items = data.get('Items', [])
            if items:
                good_items = [item['Value'] for item in items if item.get('Value') and item['Value'].get('Good') and item['Value'].get('Value') is not None]

                if good_items:
                    df = pd.DataFrame(good_items)

                    if 'Timestamp' in df.columns and 'Value' in df.columns:
                        # Multiplica os valores por 24
                        df['Value'] = (df['Value'] * 24*0.96).round(2)

                        # Ajusta fuso horário
                        df['Timestamp'] = pd.to_datetime(df['Timestamp']).dt.tz_convert('Etc/GMT+3').dt.strftime('%Y-%m-%d %H:%M:%S')

                        # Renomeia a coluna com o apelido
                        column_name = piwebapi_data.loc[piwebapi_data['WebId'] == webid, 'Apelido'].values[0]
                        df.rename(columns={'Value': column_name}, inplace=True)

                        # Mescla no DataFrame final
                        if df_totalizador.empty:
                            df_totalizador = df
                        else:
                            df_totalizador = pd.merge(df_totalizador, df, on='Timestamp', how='outer')
                    else:
                        print(f"As colunas 'Timestamp' e 'Value' não estão presentes para WebID {webid}.")
                else:
                    print(f"Não há itens válidos (Good=True) para WebID {webid}.")
            else:
                print(f"Não há itens retornados para WebID {webid}.")
    except Exception as e:
        print(f"Erro ao processar WebID {webid}: {e}")
        webids_com_erro.append(webid)

# ========================================================
# Coleta dos dados do Onedrive empresarial (link publico)
# ========================================================

#Função para baixar e criar dataframe a partir da leitura do arquivo de excel
def baixar_arquivos_onedrive(link: str, sheet_name: str) -> pd.DataFrame | None:
    try:
        if "?download=1" not in link:
            link += "?download=1"

        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(link, headers=headers)
        # Verifica se é HTML (erro comum)
        content_type = response.headers.get("Content-Type", "")
        if "text/html" in content_type.lower():
            print(f"❌ Conteúdo baixado é uma página HTML, não um arquivo Excel. Verifique o link: {link}")
            return None
        if response.status_code != 200:
            print(f"❌ Erro ao baixar o arquivo: {response.status_code} | URL: {link}")
            return None
        # Lê o conteúdo como Excel
        file = BytesIO(response.content)
        df = pd.read_excel(file, sheet_name=sheet_name, engine="openpyxl")
        return df
    except Exception as e:
        print(f"❌ Erro ao processar o arquivo: {e}")
        return None

# Carregamento das def's
dados_planta_dia_atual = baixar_arquivos_onedrive(LINK_ARQUIVO_DIA_ATUAL, sheet_name="Dados_painel_hora_hora")
dados_planta_historico = baixar_arquivos_onedrive(LINK_ARQUIVO_HISTORICO, sheet_name="BancoDadosVariáveis")

# ==============================
# Tratamento dos dados de mina
# ==============================

# Garante que as colunas estão como string e padroniza valores
df_dados_mina['origin'] = df_dados_mina['origin'].astype(str).str.strip()
df_dados_mina['destination_subarea'] = df_dados_mina['destination_subarea'].astype(str).str.strip().str.lower() #precisa converter para letra minuscula para garantir que o filtro irá funcionar corretamente
df_dados_mina['exception_type'] = df_dados_mina['exception_type'].astype(str).str.strip().str.lower() #precisa converter para letra minuscula para garantir que o filtro irá funcionar corretamente
df_dados_mina['calculated_mass'] = pd.to_numeric(df_dados_mina['calculated_mass'],errors='coerce').astype('float64') #garantir que a coluna esta como numero decimal
# Filtros aplicados um a um
df_temp = df_dados_mina[df_dados_mina['origin'].isin(['Cava Paiol', 'Cava Sul'])]
df_temp = df_temp[df_temp['destination_subarea'] != 'acesso dentro da cava']
df_dados_mina = df_temp[df_temp['exception_type'] != 'Edited_Delete'].copy()
# Arredondar as horas com minutos para hora exata
df_dados_mina.loc[:, 'hora_completa'] = pd.to_datetime(df_dados_mina['datetime_end']).dt.floor('h')
#Ajustar litologias no dataframe
df_dados_mina['material'] = df_dados_mina['material'].replace({
    'Estéril': 'Estéril',
    'Estéril-RI': 'Estéril',
    'HG': 'HG',
    'HG1': 'HG',
    'HG2': 'HG',
    'HG3': 'HG',
    'LG': 'LG',
    'LG1': 'LG',
    'LG2': 'LG',
    'LG3': 'LG',
    'LG4': 'LG',
    'LG5': 'LG',
    'MG': 'MG',
    'MW': 'MG'
})

# ===========================================================
# Tratamento dos dados de Planta via API (Massa Alimentada)
# ===========================================================

# Excluir colunas desnecessarias
df_totalizador['Timestamp'] = pd.to_datetime(df_totalizador['Timestamp'], errors='coerce')
df_totalizador = df_totalizador.drop(columns=['UnitsAbbreviation','Good','Questionable','Substituted','Annotated'])
#converter coluna timestamp para string
df_totalizador['Timestamp'] = df_totalizador['Timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S')
#Mudar o nome da coluna para garantir compatibilidade na hora de unir os Dataframes
df_totalizador.rename(columns={"Retomada - TR02 - Balança": "Moinho_Massa Alimentada Moagem_(t)"},inplace=True)

# ===============================================================
# Tratamento dos dados da planta via Onedrive (Dados Historicos)
# ===============================================================

#Eliminar colunas desnecessarias
dados_planta_historico = dados_planta_historico.loc[:, [
    "Data",
    "Hora corrigida",
    "Britagem_Massa Produzida Britagem_(t)",
    "Britagem_Justificativa de NÂO atingir a massa_(txt)",
    "Moinho_Justificativa de NÂO atingir a massa_(txt)",
    "Moinho_Justificativa do Tempo operando com taxa a menor_(txt)"
]]

# Converte a coluna "Hora corrigida" para datetime, só horário mesmo
dados_planta_historico["Hora corrigida"] = pd.to_datetime(
    dados_planta_historico["Hora corrigida"], errors="coerce", format="%H:%M:%S"
)

# Filtrar somente dados necessarios (filtrando os dados dos ultimo 33 dias para acumulação da produção do mês atual)
# Converter a coluna 'Data' para datetime (se ainda não for)
dados_planta_historico["Data"] = pd.to_datetime(dados_planta_historico["Data"], errors="coerce", dayfirst=True)

# Data de hoje (sem hora)
hoje = datetime.now().date()

# Dois dias anteriores (sem hora)
data_inicio = hoje - timedelta(days=32)
data_fim = hoje - timedelta(days=1)

# Filtra os registros
dados_planta_historico = dados_planta_historico[
    (dados_planta_historico["Data"].dt.date >= data_inicio) &
    (dados_planta_historico["Data"].dt.date <= data_fim)
]

# Converte a coluna "Hora corrigida" para datetime
dados_planta_historico["Hora corrigida"] = pd.to_datetime(
    dados_planta_historico["Hora corrigida"], errors="coerce", format="%H:%M:%S"
)
# Cria 'Timestamp' juntando Data + Hora (sem criar coluna extra) - coluna será utilizada nas proximas etapas
dados_planta_historico["Timestamp"] = pd.to_datetime(
    dados_planta_historico["Data"].dt.strftime("%Y-%m-%d") + " " +
    dados_planta_historico["Hora corrigida"].dt.strftime("%H:%M:%S"),
    format="%Y-%m-%d %H:%M:%S",
    errors="coerce"
)

# ========================================================
# Tratamento dos dados da planta via Onedrive (Dia Atual)
# ========================================================

# Garante que a coluna 'Data' esteja no formato datetime
dados_planta_dia_atual["Data"] = pd.to_datetime(
    dados_planta_dia_atual["Data"], errors="coerce", dayfirst=True
)

# Data atual (sem hora)
hoje = datetime.now().date()

# Filtra os registros que são do dia atual - aplicado para evitar que se a sala de controle demore a trocar de dia no relatorio não apareça dados duplicados em df_dados_planta
dados_planta_dia_atual = dados_planta_dia_atual[
    dados_planta_dia_atual["Data"].dt.date == hoje
]

# Converte 'Data' para datetime
dados_planta_dia_atual["Data"] = pd.to_datetime(
    dados_planta_dia_atual["Data"], errors="coerce", dayfirst=True
)

# Cria 'Timestamp' juntando Data + Hora (sem criar coluna extra)
dados_planta_dia_atual["Timestamp"] = pd.to_datetime(
    dados_planta_dia_atual["Data"].dt.strftime("%Y-%m-%d") + " " +
    dados_planta_dia_atual["Hora corrigida"].dt.strftime("%H:%M:%S"),
    format="%Y-%m-%d %H:%M:%S",
    errors="coerce"
)

# ===========================================================================
# Criação do Dataframe final com os dados da planta consolidado (PI + Excel)
# ===========================================================================

# União dos dataframes
df_dados_planta = pd.concat([dados_planta_historico, dados_planta_dia_atual], ignore_index=True)

# Garantir que ambas as colunas estejam em datetime completo (com hora)
df_dados_planta["Timestamp"] = pd.to_datetime(df_dados_planta["Timestamp"], errors="coerce")
df_totalizador["Timestamp"] = pd.to_datetime(df_totalizador["Timestamp"], errors="coerce")

# Fazer o merge usando 'Timestamp' como chave
df_dados_planta = df_totalizador.merge(
    df_dados_planta[[
        "Timestamp",
        "Britagem_Massa Produzida Britagem_(t)",
        "Britagem_Justificativa de NÂO atingir a massa_(txt)",
        "Moinho_Justificativa de NÂO atingir a massa_(txt)",
        "Moinho_Justificativa do Tempo operando com taxa a menor_(txt)"]],
    on="Timestamp",
    how="left"  
)

# Limpeza e transformação no df_dados_planta
colunas_num = ["Britagem_Massa Produzida Britagem_(t)"]
for col in colunas_num:
    df_dados_planta[col] = pd.to_numeric(df_dados_planta[col], errors="coerce")

colunas_txt = [
    "Britagem_Justificativa de NÂO atingir a massa_(txt)",
    "Moinho_Justificativa de NÂO atingir a massa_(txt)",
    "Moinho_Justificativa do Tempo operando com taxa a menor_(txt)"
]
for col in colunas_txt:
    df_dados_planta[col] = df_dados_planta[col].astype(str).replace({'nan': None, '': None})

for col in df_dados_planta.columns:
    df_dados_planta[col] = df_dados_planta[col].astype("object")

df_dados_planta = df_dados_planta.where(pd.notnull(df_dados_planta), None)

dados = df_dados_planta.to_dict(orient='records')
for i, linha in enumerate(dados):
    for chave, valor in linha.items():
        if isinstance(valor, float) and (math.isnan(valor) or math.isinf(valor)):
            raise ValueError(f"❌ Valor inválido na linha {i}, coluna '{chave}': {valor}")

# ===============================================================
# Tratamento dos dados para envio ao Supabase com fuso horário
# ===============================================================

# fuso horário Brasil‑São Paulo
tz_br = ZoneInfo("America/Sao_Paulo")

def preparar_df(df, ts_cols):
    for col in ts_cols:
        df[col] = (
            pd.to_datetime(df[col], errors='coerce')  # transforma em datetime
              .dt.tz_localize(tz_br, ambiguous='infer', nonexistent='shift_forward')  # diz que está em Brasília
              .dt.tz_convert("UTC")  # converte para UTC
        )
    return df

# prepara timestamps com fuso
df_dados_mina = preparar_df(df_dados_mina, ['datetime_end', 'hora_completa'])
df_dados_planta = preparar_df(df_dados_planta, ['Timestamp'])

# Função de envio com processamento em blocos e serialização de datetime com fuso
def enviar_dados_supabase(df, table_name, url, key, chunk_size=500):
    supabase = create_client(url, key)

    # Serialização robusta
    def serializar_valor(valor):
        if pd.isna(valor):  # Trata NaN, NaT, None
            return None
        if isinstance(valor, pd.Timestamp):
            return valor.isoformat()
        if isinstance(valor, (np.integer, np.floating)):
            return valor.item()
        if isinstance(valor, float) and (np.isnan(valor) or np.isinf(valor)):
            return None
        return valor

    # Etapa 1: substitui NaNs e aplica serialização
    df_serializado = df.apply(lambda col: col.map(serializar_valor))

    # Etapa 2: transforma em registros validados
    registros = []
    for _, row in df_serializado.iterrows():
        registro = {}
        for col, val in row.items():
            try:
                # Conversão final defensiva
                if isinstance(val, float) and (np.isnan(val) or np.isinf(val)):
                    registro[col] = None
                else:
                    registro[col] = val
            except Exception:
                registro[col] = None
        registros.append(registro)

    # Limpa a tabela
    supabase.table(table_name).delete().neq("id", 0).execute()

    # Insere em blocos
    resposta = None
    for i in range(0, len(registros), chunk_size):
        batch = registros[i:i+chunk_size]
        resposta = supabase.table(table_name).insert(batch).execute()
    return resposta

# chamadas ajustadas
resposta1 = enviar_dados_supabase(df_dados_mina, 'repositorio_mina_fuso', SUPABASE_URL, SUPABASE_KEY)
resposta2 = enviar_dados_supabase(df_dados_planta, 'repositorio_planta_fuso', SUPABASE_URL, SUPABASE_KEY)