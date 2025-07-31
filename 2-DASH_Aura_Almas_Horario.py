import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import os
import pytz

from datetime import datetime, timedelta
from dotenv import load_dotenv
from supabase import create_client, Client
from streamlit_autorefresh import st_autorefresh
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# Atualiza a cada 10 minutos
st_autorefresh(interval=600 * 1000, key="auto_refresh")

# ==============================================
# Carregamento das tabelas
# ==============================================

# Carrega as variáveis do arquivo .env
load_dotenv()

# Agora pega as variáveis pelo nome
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Inicializa cliente
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Função para ler os dados das tabelas
def ler_dados_supabase(tabela: str, pagina_tamanho: int = 1000) -> pd.DataFrame:
    offset = 0
    dados_completos = []

    while True:
        resposta = (
            supabase
            .table(tabela)
            .select("*")
            .range(offset, offset + pagina_tamanho - 1)  # define o intervalo de linhas
            .execute()
        )
        dados = resposta.data
        if not dados:
            break  # terminou de puxar todas as linhas
        dados_completos.extend(dados)
        offset += pagina_tamanho

    return pd.DataFrame(dados_completos)

# Lê dados da tabela 'movimentacao_mina'
df_dados_mina = ler_dados_supabase("repositorio_mina_fuso")
df_dados_planta = ler_dados_supabase("repositorio_planta_fuso")
    
# Renomer nomes das colunas para melhor exibição no Tooltip dos graficos
df_dados_planta.rename(columns={
    "Moinho_Justificativa do Tempo operando com taxa a menor_(txt)": "Desvio taxa Moagem",
    "Britagem_Justificativa de NÂO atingir a massa_(txt)": "Justificativa Alimentação Britagem",
    "Moinho_Justificativa de NÂO atingir a massa_(txt)": "Justificativa Alimentação Moagem"
}, inplace=True)

# ==============================================
# Funções de agregação
# ==============================================

#Parametros para filtrar os dados dos graficos com base nas ultimas 24 horas
# Define fuso horário
tz_br = ZoneInfo("America/Sao_Paulo")
tz_utc = ZoneInfo("UTC")

# Agora (em Brasília)
agora_brasilia = datetime.now(tz_br)

# Ajusta para hora cheia anterior (ex: se for 15h27 → considera 14h)
agora_brasilia = agora_brasilia.replace(minute=0, second=0, microsecond=0)

# Parâmetros em BRT
parametro_inicio_brt = agora_brasilia - timedelta(hours=24)
parametro_fim_brt = agora_brasilia

# Converte para UTC
parametro_inicio = parametro_inicio_brt.astimezone(tz_utc)
parametro_agora = parametro_fim_brt.astimezone(tz_utc)

# Função para agregar dados por hora
def agregar_por_hora(
    df,
    valor_coluna,
    coluna_hora='hora_completa',
    grupo_material=None,
    tipo_agregacao='sum',
    colunas_texto=None
):
    # Se DataFrame vazio ou coluna de hora ausente
    if df is None or df.empty or coluna_hora not in df.columns:
        return pd.DataFrame(columns=['hora', 'valor'] + (colunas_texto or []))

    df_filtrado = df.copy()

    # Converte para datetime com UTC; se não der, transforma em NaT
    df_filtrado[coluna_hora] = pd.to_datetime(df_filtrado[coluna_hora], utc=True, errors='coerce')

    # Remove valores NaT
    df_filtrado = df_filtrado[df_filtrado[coluna_hora].notna()]

    # Se após isso o DataFrame estiver vazio, retorna um df limpo
    if df_filtrado.empty:
        return pd.DataFrame(columns=['hora', 'valor'] + (colunas_texto or []))

    # Floor para hora cheia
    df_filtrado[coluna_hora] = df_filtrado[coluna_hora].dt.floor('h')

    # Garantir que os parâmetros globais existam
    if 'parametro_inicio' not in globals() or 'parametro_agora' not in globals():
        raise ValueError("Parâmetros globais 'parametro_inicio' e 'parametro_agora' não estão definidos.")

    # Filtra pelo intervalo de tempo
    inicio = parametro_inicio
    agora = parametro_agora
    df_filtrado = df_filtrado[(df_filtrado[coluna_hora] >= inicio) & (df_filtrado[coluna_hora] < agora)]

    # Filtra por grupo, se necessário
    if grupo_material is not None and 'material_group' in df_filtrado.columns:
        df_filtrado = df_filtrado[df_filtrado['material_group'] == grupo_material]

    if df_filtrado.empty:
        return pd.DataFrame(columns=['hora', 'valor'] + (colunas_texto or []))

    # Define colunas a manter
    colunas_agregadas = [coluna_hora, valor_coluna]
    if colunas_texto:
        colunas_agregadas += colunas_texto

    # Filtra somente colunas existentes para evitar KeyError
    colunas_agregadas = [col for col in colunas_agregadas if col in df_filtrado.columns]
    df_filtrado = df_filtrado[colunas_agregadas]

    # Agregação segura
    df_agrupado = (
        df_filtrado
        .groupby(coluna_hora)
        .agg({valor_coluna: tipo_agregacao, **{col: 'first' for col in (colunas_texto or []) if col in df_filtrado.columns}})
        .reset_index()
        .rename(columns={coluna_hora: 'hora', valor_coluna: 'valor'})
    )
    return df_agrupado

# Função apra agregar dados a serem usados com a função de grafico empilhado
def agregar_por_hora_empilhado(
    df,
    valor_coluna,
    coluna_hora='hora_completa',
    coluna_empilhamento='material',
    tipo_agregacao='sum'
):
    # Valida se DataFrame está vazio ou se coluna de hora não existe
    if df is None or df.empty or coluna_hora not in df.columns or valor_coluna not in df.columns:
        return pd.DataFrame(columns=['hora', 'categoria', 'valor'])

    df_filtrado = df.copy()

    # Converte a coluna de hora para datetime com fuso UTC
    df_filtrado[coluna_hora] = pd.to_datetime(df_filtrado[coluna_hora], utc=True, errors='coerce')

    # Remove registros com hora inválida
    df_filtrado = df_filtrado[df_filtrado[coluna_hora].notna()]

    if df_filtrado.empty:
        return pd.DataFrame(columns=['hora', 'categoria', 'valor'])

    df_filtrado[coluna_hora] = df_filtrado[coluna_hora].dt.floor('h')

    # Parâmetros globais de tempo
    if 'parametro_inicio' not in globals() or 'parametro_agora' not in globals():
        raise ValueError("Parâmetros globais 'parametro_inicio' e 'parametro_agora' não definidos.")

    inicio = parametro_inicio
    agora = parametro_agora

    # Filtro de intervalo
    df_filtrado = df_filtrado[(df_filtrado[coluna_hora] >= inicio) & (df_filtrado[coluna_hora] < agora)]

    if df_filtrado.empty:
        return pd.DataFrame(columns=['hora', 'categoria', 'valor'])

    # Valida se coluna de empilhamento existe
    if coluna_empilhamento not in df_filtrado.columns:
        return pd.DataFrame(columns=['hora', 'categoria', 'valor'])

    # Agregação segura
    df_agrupado = (
        df_filtrado
        .groupby([coluna_hora, coluna_empilhamento])[valor_coluna]
        .agg(tipo_agregacao)
        .reset_index()
        .rename(columns={
            coluna_hora: 'hora',
            coluna_empilhamento: 'categoria',
            valor_coluna: 'valor'
        })
    )
    return df_agrupado

# ==============================================
# Funções para gerar graficos
# ==============================================

# Função para Criar os graficos de barras
def gerar_grafico_colunas(
    df_agrupado,
    valor_referencia=None,
    titulo='Título do Gráfico',
    yaxis_min=None,
    yaxis_max=None,
    colunas_tooltip=None
):
    tz_br = ZoneInfo("America/Sao_Paulo")
    df_plot = df_agrupado.copy() if df_agrupado is not None else pd.DataFrame(columns=['hora', 'valor'])

    for col in ['hora', 'valor']:
        if col not in df_plot.columns:
            df_plot[col] = pd.NaT if col == 'hora' else 0

    # Garante que hora está com timezone e converte para horário de Brasília
    df_plot['hora'] = pd.to_datetime(df_plot['hora'], utc=True, errors='coerce')
    df_plot['hora_br'] = df_plot['hora'].dt.tz_convert(tz_br)

    # Usa hora convertida para formatação e lógica de troca de dia
    df_plot['hora_str'] = df_plot['hora_br'].dt.strftime('%H')
    df_plot['data'] = df_plot['hora_br'].dt.strftime('%d/%m')

    colunas_tooltip = colunas_tooltip or []
    customdata = []

    for _, row in df_plot.iterrows():
        linha_tooltip = []
        for col in colunas_tooltip:
            valor = row.get(col)
            if pd.notnull(valor):
                linha_tooltip.append(f"<b>{col}</b>: {valor}")
        linha_tooltip.insert(0, f"<b>Data</b>: {row['data']}")
        linha_tooltip.append(f"<b>Hora</b>: {row['hora_str']}")
        linha_tooltip.append(f"<b>Valor</b>: {row['valor']:,.0f}".replace(",", "X").replace(".", ",").replace("X", "."))
        customdata.append("<br>".join(linha_tooltip))

    if valor_referencia is not None:
        df_plot['cor'] = df_plot['valor'].apply(lambda x: "#F4614D" if x < valor_referencia else "#2D3D70")
    else:
        df_plot['cor'] = "#2D3D70"

    dia_hoje_br = datetime.now(tz_br).date()
    troca_idx = df_plot[df_plot['hora_br'].dt.date == dia_hoje_br].index.min()
    if pd.isna(troca_idx):
        troca_idx = None

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df_plot['hora_str'],
        y=df_plot['valor'],
        marker_color=df_plot['cor'],
        text=[f"{v:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".") for v in df_plot['valor']],
        textposition="inside",
        textangle=270,
        textfont=dict(color='white', size=25),
        hovertemplate="%{customdata}<extra></extra>",
        customdata=customdata
    ))

    if valor_referencia is not None:
        fig.add_hline(
            y=valor_referencia,
            line_dash="dash",
            line_color="black",
            annotation_text=f"Meta: {valor_referencia:,.0f}".replace(",", "."),
            annotation_position="top right",
            annotation_font_size=12,
            annotation_font_color="black",
            annotation_yshift=50
        )

    if troca_idx is not None and troca_idx > 0:
        fig.add_vline(
            x=troca_idx - 0.5,
            line_dash="solid",
            line_color="black",
            line_width=2
        )
        fig.add_annotation(
            x=troca_idx - 1.5,
            y=1.09,
            xref='x',
            yref='paper',
            text=(dia_hoje_br - timedelta(days=1)).strftime('%d/%m'),
            showarrow=False,
            yanchor="top",
            font=dict(size=14, color="black")
        )
        fig.add_annotation(
            x=troca_idx + 0.5,
            y=1.09,
            xref='x',
            yref='paper',
            text=dia_hoje_br.strftime('%d/%m'),
            showarrow=False,
            yanchor="top",
            font=dict(size=14, color="black")
        )

    fig.update_layout(
        title=dict(text=titulo, x=0.0, xanchor='left', font=dict(size=20, family='Arial', color='black')),
        xaxis=dict(tickangle=0, type='category', tickfont=dict(size=16, family='Arial', color='black'), showline=True, linecolor='black'),
        yaxis=dict(visible=False, range=[yaxis_min, yaxis_max] if yaxis_min is not None and yaxis_max is not None else None),
        bargap=0.2,
        margin=dict(t=40, b=20, l=0, r=0),
        plot_bgcolor='white',
        paper_bgcolor='white',
        height=300
    )
    return fig

# Função para criar grafico de barras empilhadas
def gerar_grafico_empilhado(
    df_agrupado,
    titulo='Título do Gráfico',
    legenda_yshift=20,
    yaxis_min=None,
    yaxis_max=None,
    cores_categorias=None,
    tooltip_template=None
):
    tz_br = ZoneInfo("America/Sao_Paulo")
    df_plot = df_agrupado.copy()

    if df_plot.empty or 'hora' not in df_plot.columns or 'categoria' not in df_plot.columns:
        fig = go.Figure()
        fig.update_layout(
            title=dict(text=f"{titulo} (sem dados disponíveis)", x=0.0, xanchor='left',
                       font=dict(size=20, family='Arial', color='gray')),
            plot_bgcolor='white',
            paper_bgcolor='white',
            height=300
        )
        return fig

    df_plot['hora'] = pd.to_datetime(df_plot['hora'], utc=True, errors='coerce')
    df_plot['hora_br'] = df_plot['hora'].dt.tz_convert(tz_br)
    df_plot = df_plot.sort_values(['hora_br', 'categoria'])

    df_plot['hora_str'] = df_plot['hora_br'].dt.strftime('%H')
    categorias_x = list(dict.fromkeys(df_plot.sort_values('hora_br')['hora_str'].tolist()))
    df_plot['hora_str'] = pd.Categorical(df_plot['hora_str'], categories=categorias_x, ordered=True)

    dia_hoje_br = datetime.now(tz_br).date()
    troca_hora = df_plot[df_plot['hora_br'].dt.date == dia_hoje_br]['hora_str'].min()
    troca_hora_str = troca_hora if not pd.isna(troca_hora) else None

    if cores_categorias is None:
        cores_categorias = {
            'Estéril': '#AAAAAA',
            'HG': '#FF5733',
            'MG': '#FFC300',
            'LG': '#4CAF50',
            'HL': '#2D3D70'
        }

    if tooltip_template is None:
        tooltip_template = (
            "<b>Hora</b>: %{x}<br>"
            "<b>Valor</b>: %{y:,.0f}<br>"
            "<b>Categoria</b>: %{customdata[0]}<extra></extra>"
        )

    fig = go.Figure()
    for cat, cor in cores_categorias.items():
        df_cat = df_plot[df_plot['categoria'] == cat]
        if not df_cat.empty:
            fig.add_trace(go.Bar(
                x=df_cat['hora_str'],
                y=df_cat['valor'],
                name=cat,
                marker_color=cor,
                customdata=df_cat[['categoria']],
                hovertemplate=tooltip_template,
                showlegend=True
            ))

    df_totais = df_plot.groupby('hora_str', observed=True)['valor'].sum().reset_index()
    df_totais['texto'] = df_totais['valor'].apply(lambda v: f"{v/1000:,.1f}".replace(",", "X").replace(".", ",").replace("X", "."))

    fig.add_trace(go.Scatter(
        x=df_totais['hora_str'],
        y=df_totais['valor'] + (df_totais['valor'].max() * 0.03 if df_totais['valor'].max() > 0 else 1),
        mode='text',
        text=df_totais['texto'],
        textposition='top center',
        showlegend=False,
        textfont=dict(size=14, color='black')
    ))

    if troca_hora_str in categorias_x:
        troca_pos = categorias_x.index(troca_hora_str)

        fig.add_vline(
            x=troca_pos - 0.5,
            line_dash="solid",
            line_color="black",
            line_width=2
        )
        fig.add_annotation(
            x=troca_pos - 1.5,
            y=1.09,
            xref='x',
            yref='paper',
            text=(dia_hoje_br - timedelta(days=1)).strftime('%d/%m'),
            showarrow=False,
            yanchor="top",
            font=dict(size=14, color="black")
        )
        fig.add_annotation(
            x=troca_pos + 0.5,
            y=1.09,
            xref='x',
            yref='paper',
            text=dia_hoje_br.strftime('%d/%m'),
            showarrow=False,
            yanchor="top",
            font=dict(size=14, color="black")
        )

    fig.update_layout(
        barmode='stack',
        title=dict(text=titulo, x=0.0, xanchor='left', font=dict(size=20, family='Arial', color='black')),
        xaxis=dict(
            tickangle=0,
            type='category',
            categoryorder='array',
            categoryarray=categorias_x,
            tickfont=dict(size=16, family='Arial', color='black'),
            showline=True,
            linecolor='black'
        ),
        yaxis=dict(
            visible=False,
            range=[yaxis_min, yaxis_max] if yaxis_min is not None and yaxis_max is not None else None
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=0.85 + (legenda_yshift / 100),
            xanchor="center",
            x=0.8,
            font=dict(size=14)
        ),
        bargap=0.2,
        margin=dict(t=30, b=20, l=0, r=0),
        plot_bgcolor='white',
        paper_bgcolor='white',
        height=300
    )
    return fig

# =========================================
# Criação dos Graficos MINA - Movimentação
# =========================================

# Selecionar def com o conteudo
df_base_mina = df_dados_mina

# Grafico 1 - Contagem de Viajens
df_agg_viagens = agregar_por_hora(
    df=df_base_mina,
    valor_coluna='calculated_mass',
    grupo_material=None,
    tipo_agregacao= 'count'
)

grafico_numero_viagens = gerar_grafico_colunas(
    df_agrupado=df_agg_viagens,
    valor_referencia=71,
    titulo='Viagens (n°)',
    yaxis_min=0
)

# Grafico 2 - Movimentação Total por litologia
# Define cores e ordem desejada
cores_customizadas = {
    'Estéril': '#AAAAAA',
    'LG': '#4CAF50',
    'MG': '#FFC300',
    'HG': '#FF5733',
    'HL': '#2D3D70'
}

# Agrega os dados
df_agg_movimentacao_litologia = agregar_por_hora_empilhado(
    df=df_base_mina,
    valor_coluna='calculated_mass',
    coluna_empilhamento='material',
    tipo_agregacao='sum'
)

# Gera gráfico com tooltip customizado
grafico_movimentacao_litogia = gerar_grafico_empilhado(
    df_agrupado=df_agg_movimentacao_litologia,
    titulo='Movimentação por litologia (Kt)',
    legenda_yshift=20,
    yaxis_min=0,
    yaxis_max=5000,
    cores_categorias=cores_customizadas,
    #tooltip_template="<b>Material</b>: %{customdata[0]}<br><b>Hora</b>: %{x}h<br><b>Valor</b>: %{y:,} toneladas<extra></extra>"
    tooltip_template = "<b>Material</b>: %{customdata[0]}<br><b>Hora</b>: %{x}h<br><b>Valor</b>: %{y:,.2f} toneladas<extra></extra>"

)

# =====================================
# Criação dos Graficos Planta 
# =====================================

# Selecionar def com o conteudo
df_base_planta = df_dados_planta

# Grafico 1 - Alimentação Britagem
df_agg_britagem = agregar_por_hora(
    df=df_base_planta,
    coluna_hora='Timestamp',
    valor_coluna='Britagem_Massa Produzida Britagem_(t)',
    tipo_agregacao='sum',
    colunas_texto=['Justificativa Alimentação Britagem']
)

grafico_barra_britagem = gerar_grafico_colunas(
    df_agrupado=df_agg_britagem,
    valor_referencia=310,  
    titulo='Alimentação Britagem (t)',
    yaxis_min=0,
    yaxis_max=470,
    colunas_tooltip=['Justificativa Alimentação Britagem']
)

# Grafico 2 - Alimentação Moagem
df_agg_moagem = agregar_por_hora(
    df=df_base_planta,
    coluna_hora='Timestamp',
    valor_coluna='Moinho_Massa Alimentada Moagem_(t)',
    tipo_agregacao='sum',
    colunas_texto=['Justificativa Alimentação Moagem','Desvio taxa Moagem']
)

grafico_barra_moagem = gerar_grafico_colunas(
    df_agrupado=df_agg_moagem,
    valor_referencia=250,  
    titulo='Alimentação Moagem (t)',
    yaxis_min=0,
    yaxis_max=470,
    colunas_tooltip=['Justificativa Alimentação Moagem','Desvio taxa Moagem']
)

# =======================================================================
# Funções para Calculos de Ritmo, Produção Acumulada e Ritmo de Produção
# =======================================================================

# Função para calcular o acumulado mensal
def acumulado_mensal(
    df: pd.DataFrame,
    coluna_valor: str,
    coluna_datahora: str,
    tipo_agregacao: str = 'sum'
) -> float:
    # Retorno padrão para casos sem dados ou colunas ausentes
    if df is None or df.empty or coluna_valor not in df.columns or coluna_datahora not in df.columns:
        return 0.0

    # Define fuso horário local (São Paulo)
    fuso = pytz.timezone('America/Sao_Paulo')
    agora = datetime.now(fuso)

    # Se for dia 1, considera mês anterior como referência
    data_base = agora - timedelta(days=1) if agora.day == 1 else agora
    mes = data_base.month
    ano = data_base.year

    # Conversão segura da coluna de data/hora para datetime com fuso horário
    try:
        df[coluna_datahora] = pd.to_datetime(df[coluna_datahora], errors='coerce')
        df = df[df[coluna_datahora].notna()]  # remove inválidos

        if df.empty:
            return 0.0

        # Torna timezone-aware com fuso de SP
        if df[coluna_datahora].dt.tz is None:
            df[coluna_datahora] = df[coluna_datahora].dt.tz_localize(fuso)
        else:
            df[coluna_datahora] = df[coluna_datahora].dt.tz_convert(fuso)
    except Exception as e:
        raise ValueError(f"Erro ao processar a coluna '{coluna_datahora}' como datetime: {e}")

    # Filtra registros do mês e ano de interesse
    df_filtrado = df[
        (df[coluna_datahora].dt.month == mes) &
        (df[coluna_datahora].dt.year == ano)
    ]

    if df_filtrado.empty:
        return 0.0

    # Agregação segura
    tipo_agregacao = tipo_agregacao.lower()
    if tipo_agregacao == 'sum':
        return df_filtrado[coluna_valor].sum()
    elif tipo_agregacao == 'mean':
        return df_filtrado[coluna_valor].mean()
    elif tipo_agregacao == 'max':
        return df_filtrado[coluna_valor].max()
    elif tipo_agregacao == 'min':
        return df_filtrado[coluna_valor].min()
    elif tipo_agregacao == 'count':
        return df_filtrado[coluna_valor].count()
    else:
        raise ValueError(f"Tipo de agregação '{tipo_agregacao}' não suportado.")
    
# Função para calcular o valor do dia anterior
def acumulado_dia_anterior(
    df: pd.DataFrame,
    coluna_valor: str,
    coluna_datahora: str,
    tipo_agregacao: str = 'sum'
) -> float:
    # Validação inicial do DataFrame e colunas
    if df is None or df.empty or coluna_valor not in df.columns or coluna_datahora not in df.columns:
        return 0.0

    # Fuso horário local (São Paulo)
    fuso = pytz.timezone('America/Sao_Paulo')
    agora = datetime.now(fuso)
    ontem = (agora - timedelta(days=1)).date()

    # Conversão segura da coluna de data/hora para datetime com fuso horário
    try:
        df[coluna_datahora] = pd.to_datetime(df[coluna_datahora], errors='coerce')
        df = df[df[coluna_datahora].notna()]  # Remove registros inválidos

        if df.empty:
            return 0.0

        # Torna timezone-aware
        if df[coluna_datahora].dt.tz is None:
            df[coluna_datahora] = df[coluna_datahora].dt.tz_localize(fuso)
        else:
            df[coluna_datahora] = df[coluna_datahora].dt.tz_convert(fuso)

    except Exception as e:
        raise ValueError(f"Erro ao processar a coluna '{coluna_datahora}' como datetime: {e}")

    # Filtra registros do dia anterior
    df['data'] = df[coluna_datahora].dt.date
    df_filtrado = df[df['data'] == ontem]

    if df_filtrado.empty:
        return 0.0

    # Agregação segura
    tipo_agregacao = tipo_agregacao.lower()
    if tipo_agregacao == 'sum':
        return df_filtrado[coluna_valor].sum()
    elif tipo_agregacao == 'mean':
        return df_filtrado[coluna_valor].mean()
    elif tipo_agregacao == 'max':
        return df_filtrado[coluna_valor].max()
    elif tipo_agregacao == 'min':
        return df_filtrado[coluna_valor].min()
    elif tipo_agregacao == 'count':
        return df_filtrado[coluna_valor].count()
    else:
        raise ValueError(f"Tipo de agregação '{tipo_agregacao}' não suportado.")
    
# Função para calcular o acumulado do dia atual
def acumulado_dia_atual(
    df: pd.DataFrame,
    coluna_valor: str,
    coluna_datahora: str,
    tipo_agregacao: str = 'sum'
) -> float:
    # Validação inicial
    if df is None or df.empty or coluna_valor not in df.columns or coluna_datahora not in df.columns:
        return 0.0

    fuso = pytz.timezone('America/Sao_Paulo')
    agora = datetime.now(fuso)
    hoje = agora.date()

    # Conversão segura da coluna de data/hora
    try:
        df[coluna_datahora] = pd.to_datetime(df[coluna_datahora], errors='coerce')
        df = df[df[coluna_datahora].notna()]
        if df.empty:
            return 0.0

        # Torna timezone-aware
        if df[coluna_datahora].dt.tz is None:
            df[coluna_datahora] = df[coluna_datahora].dt.tz_localize(fuso)
        else:
            df[coluna_datahora] = df[coluna_datahora].dt.tz_convert(fuso)

    except Exception as e:
        raise ValueError(f"Erro ao processar a coluna '{coluna_datahora}' como datetime: {e}")

    # Filtragem por data atual
    df['data'] = df[coluna_datahora].dt.date
    df_filtrado = df[df['data'] == hoje]

    if df_filtrado.empty:
        return 0.0

    # Agregação
    tipo_agregacao = tipo_agregacao.lower()
    if tipo_agregacao == 'sum':
        return df_filtrado[coluna_valor].sum()
    elif tipo_agregacao == 'mean':
        return df_filtrado[coluna_valor].mean()
    elif tipo_agregacao == 'max':
        return df_filtrado[coluna_valor].max()
    elif tipo_agregacao == 'min':
        return df_filtrado[coluna_valor].min()
    elif tipo_agregacao == 'count':
        return df_filtrado[coluna_valor].count()
    else:
        raise ValueError(f"Tipo de agregação '{tipo_agregacao}' não suportado.")


# Função para calcular o ritmo de produção
def ritmo_mensal(
    df: pd.DataFrame,
    coluna_valor: str,
    coluna_datahora: str,
    tipo_agregacao: str = 'sum'
) -> float:
    if df is None or df.empty or coluna_valor not in df.columns or coluna_datahora not in df.columns:
        return 0.0

    fuso = pytz.timezone('America/Sao_Paulo')
    agora = datetime.now(fuso)

    # Se for dia 1, usa o mês anterior como base
    data_base = agora - timedelta(days=1) if agora.day == 1 else agora
    mes, ano = data_base.month, data_base.year

    try:
        df[coluna_datahora] = pd.to_datetime(df[coluna_datahora], errors='coerce')
        df = df[df[coluna_datahora].notna()]
        if df.empty:
            return 0.0

        # Garantir timezone-aware
        if df[coluna_datahora].dt.tz is None:
            df[coluna_datahora] = df[coluna_datahora].dt.tz_localize(fuso)
        else:
            df[coluna_datahora] = df[coluna_datahora].dt.tz_convert(fuso)

    except Exception as e:
        raise ValueError(f"Erro ao processar a coluna '{coluna_datahora}' como datetime: {e}")

    # Filtragem para o mês em questão
    df_mes = df[
        (df[coluna_datahora].dt.month == mes) &
        (df[coluna_datahora].dt.year == ano)
    ]

    if df_mes.empty:
        return 0.0

    # Agregação
    tipo_agregacao = tipo_agregacao.lower()
    if tipo_agregacao == 'sum':
        acumulado = df_mes[coluna_valor].sum()
    elif tipo_agregacao == 'mean':
        acumulado = df_mes[coluna_valor].mean()
    elif tipo_agregacao == 'max':
        acumulado = df_mes[coluna_valor].max()
    elif tipo_agregacao == 'min':
        acumulado = df_mes[coluna_valor].min()
    elif tipo_agregacao == 'count':
        acumulado = df_mes[coluna_valor].count()
    else:
        raise ValueError(f"Tipo de agregação '{tipo_agregacao}' não suportado.")

    # Cálculo do ritmo projetado para o fim do mês
    data_min = df_mes[coluna_datahora].min()
    data_max = agora.replace(minute=0, second=0, microsecond=0)
    horas_decorridas = int((data_max - data_min).total_seconds() // 3600)

    inicio_mes = pd.Timestamp(datetime(ano, mes, 1), tz=fuso)
    fim_mes = (inicio_mes + pd.offsets.MonthBegin(1)) - timedelta(seconds=1)
    total_horas_mes = int((fim_mes - inicio_mes).total_seconds() // 3600) + 1

    if horas_decorridas == 0 or (total_horas_mes - horas_decorridas) <= 0:
        return acumulado

    ritmo = ((acumulado / horas_decorridas) * (total_horas_mes - horas_decorridas)) + acumulado
    return ritmo

# Função para calcular o ritmo do dia atual
def ritmo_dia_atual(
    df: pd.DataFrame,
    coluna_valor: str,
    coluna_datahora: str,
    tipo_agregacao: str = 'sum'
) -> float:
    if df is None or df.empty or coluna_valor not in df.columns or coluna_datahora not in df.columns:
        return 0.0

    fuso = pytz.timezone('America/Sao_Paulo')
    agora = datetime.now(fuso).replace(minute=0, second=0, microsecond=0)
    hoje = agora.date()

    try:
        df[coluna_datahora] = pd.to_datetime(df[coluna_datahora], errors='coerce')
        df = df[df[coluna_datahora].notna()]
        if df.empty:
            return 0.0

        if df[coluna_datahora].dt.tz is None:
            df[coluna_datahora] = df[coluna_datahora].dt.tz_localize(fuso)
        else:
            df[coluna_datahora] = df[coluna_datahora].dt.tz_convert(fuso)

    except Exception as e:
        raise ValueError(f"Erro ao processar a coluna '{coluna_datahora}' como datetime: {e}")

    # Filtra dados do dia atual até a hora corrente (sem incluir a hora atual)
    df_dia = df[
        (df[coluna_datahora].dt.date == hoje) &
        (df[coluna_datahora].dt.hour < agora.hour)
    ]

    if df_dia.empty:
        return 0.0

    # Agregação do acumulado até o momento
    tipo_agregacao = tipo_agregacao.lower()
    if tipo_agregacao == 'sum':
        acumulado = df_dia[coluna_valor].sum()
    elif tipo_agregacao == 'mean':
        acumulado = df_dia[coluna_valor].mean()
    elif tipo_agregacao == 'max':
        acumulado = df_dia[coluna_valor].max()
    elif tipo_agregacao == 'min':
        acumulado = df_dia[coluna_valor].min()
    elif tipo_agregacao == 'count':
        acumulado = df_dia[coluna_valor].count()
    else:
        raise ValueError(f"Tipo de agregação '{tipo_agregacao}' não suportado.")

    horas_decorridas = agora.hour
    total_horas_dia = 24

    if horas_decorridas == 0 or total_horas_dia - horas_decorridas == 0:
        return acumulado

    # Projeção do ritmo até o final do dia
    ritmo = ((acumulado / horas_decorridas) * (total_horas_dia - horas_decorridas)) + acumulado
    return ritmo

# =============================================
# Chamada das funções de cálculo
# =============================================

# Chamada das função de Acumulado do mês
# =============================================

# 1 - Acumulado Movimentação mina do mês
valor_mensal_movimentacao_mina = acumulado_mensal(
    df=df_dados_mina,
    coluna_valor='calculated_mass',
    coluna_datahora='hora_completa',
    tipo_agregacao='sum'
)

# 2 - Acumulado Viagens mina do mês
valor_mensal_viagens = acumulado_mensal(
    df=df_dados_mina,
    coluna_valor='calculated_mass',
    coluna_datahora='hora_completa',
    tipo_agregacao='count'
)

# 3 - Acumulado Britagem do mês
valor_mensal_britagem = acumulado_mensal(
    df=df_dados_planta,
    coluna_valor='Britagem_Massa Produzida Britagem_(t)',
    coluna_datahora='Timestamp',
    tipo_agregacao='sum'
)

# 4 - Acumulado Moagem do mês
valor_mensal_moagem = acumulado_mensal(
    df=df_dados_planta,
    coluna_valor='Moinho_Massa Alimentada Moagem_(t)',
    coluna_datahora='Timestamp',
    tipo_agregacao='sum'
)

# Chamada das função de Ritmo do mês
# ===================================

# 1 - Ritmo Britagem do mês
ritmo_movimentacao = ritmo_mensal(
    df=df_dados_mina,
    coluna_valor='calculated_mass',
    coluna_datahora='hora_completa',
    tipo_agregacao='sum'
)

# 2 - Ritmo Britagem do mês
ritmo_viagens = ritmo_mensal(
    df=df_dados_mina,
    coluna_valor='calculated_mass',
    coluna_datahora='hora_completa',
    tipo_agregacao='count'
)

# 3 - Ritmo Britagem do mês
ritmo_britagem = ritmo_mensal(
    df=df_dados_planta,
    coluna_valor='Britagem_Massa Produzida Britagem_(t)',
    coluna_datahora='Timestamp',
    tipo_agregacao='sum'
)

# 4 - Ritmo Moagem do mês
ritmo_moagem = ritmo_mensal(
    df=df_dados_planta,
    coluna_valor='Moinho_Massa Alimentada Moagem_(t)',
    coluna_datahora='Timestamp',
    tipo_agregacao='sum'
)

# Chamada das função dia anterior
# ================================

# 1 - Dia anterior movimentação de mina
valor_ontem_movimentacao = acumulado_dia_anterior(
    df=df_dados_mina,
    coluna_valor='calculated_mass',
    coluna_datahora='hora_completa',
    tipo_agregacao='sum'
)

# 2 - Dia anterior movimentação de mina
valor_ontem_viagens= acumulado_dia_anterior(
    df=df_dados_mina,
    coluna_valor='calculated_mass',
    coluna_datahora='hora_completa',
    tipo_agregacao='count'
)

# 3 - Dia anterior Britagem
valor_ontem_britagem = acumulado_dia_anterior(
    df=df_dados_planta,
    coluna_valor='Britagem_Massa Produzida Britagem_(t)',
    coluna_datahora='Timestamp',
    tipo_agregacao='sum'
)

# 4 - Dia anterior Britagem
valor_ontem_moagem = acumulado_dia_anterior(
    df=df_dados_planta,
    coluna_valor='Moinho_Massa Alimentada Moagem_(t)',
    coluna_datahora='Timestamp',
    tipo_agregacao='sum'
)

# Chamada das função soma do dia Atual
# =====================================

# 1 - Dia atual movimentação de mina
valor_hoje_movimentacao = acumulado_dia_atual(
    df=df_dados_mina,
    coluna_valor='calculated_mass',
    coluna_datahora='hora_completa',
    tipo_agregacao='sum'
)

# 2 - Dia atual movimentação de mina
valor_hoje_viagens= acumulado_dia_atual(
    df=df_dados_mina,
    coluna_valor='calculated_mass',
    coluna_datahora='hora_completa',
    tipo_agregacao='count'
)

# 3 - Dia atual Britagem
valor_hoje_britagem = acumulado_dia_atual(
    df=df_dados_planta,
    coluna_valor='Britagem_Massa Produzida Britagem_(t)',
    coluna_datahora='Timestamp',
    tipo_agregacao='sum'
)

# 4 - Dia atual Britagem
valor_hoje_moagem = acumulado_dia_atual(
    df=df_dados_planta,
    coluna_valor='Moinho_Massa Alimentada Moagem_(t)',
    coluna_datahora='Timestamp',
    tipo_agregacao='sum'
)

# Chamada das funções de Ritmo do dia atual
# ==========================================

# 1 - Ritmo Britagem do dia
ritmo_movimentacao_dia = ritmo_dia_atual(
    df=df_dados_mina,
    coluna_valor='calculated_mass',
    coluna_datahora='hora_completa',
    tipo_agregacao='sum'
)

# 2 - Ritmo Britagem do dia
ritmo_viagens_dia = ritmo_dia_atual(
    df=df_dados_mina,
    coluna_valor='calculated_mass',
    coluna_datahora='hora_completa',
    tipo_agregacao='count'
)

# 3 - Ritmo Britagem do dia
ritmo_britagem_dia = ritmo_dia_atual(
    df=df_dados_planta,
    coluna_valor='Britagem_Massa Produzida Britagem_(t)',
    coluna_datahora='Timestamp',
    tipo_agregacao='sum'
)

# 4 - Ritmo Moagem do dia
ritmo_moagem_dia = ritmo_dia_atual(
    df=df_dados_planta,
    coluna_valor='Moinho_Massa Alimentada Moagem_(t)',
    coluna_datahora='Timestamp',
    tipo_agregacao='sum'
)

# =============================================
# Dashboard em Streamlit - Desenvolvimento
# =============================================

# ========== Configuração da página ==========
st.set_page_config(layout="wide")

# ========== Estilo CSS otimizado para Full HD ==========
st.markdown("""
    <style>
        .block-container {
            padding-top: 0rem !important;
            padding-bottom: 0rem !important;
            max-width: 1900px;
            margin: auto;
        }
        header, .main {
            padding-top: 0rem !important;
        }
        h1, h2, h3 {
            margin-top: -10px !important;
            margin-bottom: 0px !important;
            padding: 0 !important;
        }
        .stPlotlyChart {
            padding: 0 !important;
            margin: 0 !important;
        }
    </style>
""", unsafe_allow_html=True)

# Carregamento dos icones
#=========================
# Importa os base64 já prontos
from imagens_base64 import (
    logo_aura,
    logo_mina,
    logo_moagem,
    logo_kpi
)

# Função utilitária para separar base64 e MIME
def extrair_base64_e_mime(data_uri: str):
    tipo, base64_data = data_uri.split(",", 1)
    mime = tipo.split(":")[1].split(";")[0]
    return base64_data, mime

# Extração dos dados e tipos
base64_esquerda, tipo_esquerda = extrair_base64_e_mime(logo_mina)
base64_esquerda2, tipo_esquerda2 = extrair_base64_e_mime(logo_moagem)
base64_direita, tipo_direita = extrair_base64_e_mime(logo_aura)
base64_kpi, tipo_kpi = extrair_base64_e_mime(logo_kpi)

# Funções para Exibição de KPIs Customizados
#============================================

# Função para exibir KPIs customizados
def exibir_kpis_customizados(
    valores: dict,
    imagem_base64: str = None,
    imagem_tipo: str = None,
    cor_valor: str = "#2D3D70",
    cor_label: str = "#555",
    fonte_valor: str = "22px",
    fonte_label: str = "16px",
    altura_imagem: str = "64px",
    largura_imagem: str = "64px",
    margin_top: str = "0px",
    margin_bottom: str = "10px",
    padding_top_imagem: str = "0px"  # << novo parâmetro para ajustar posição vertical da imagem
):
    # Duas colunas principais: imagem (esquerda) + kpis (direita)
    col_img, col_kpis = st.columns([1, 6])  # Ajuste a proporção se necessário

    # Bloco da imagem única
    if imagem_base64 and imagem_tipo:
        with col_img:
            st.markdown(
                f"""
                <div style="display: flex; justify-content: center; padding-top: {padding_top_imagem}; margin-top: {margin_top}; margin-bottom: {margin_bottom};">
                    <img src="data:{imagem_tipo};base64,{imagem_base64}"
                         style="height: {altura_imagem}; width: {largura_imagem}; object-fit: contain;" />
                </div>
                """,
                unsafe_allow_html=True
            )

    # Bloco dos KPIs
    with col_kpis:
        num_kpis = len(valores)
        colunas = st.columns(num_kpis)

        for i, (label, valor) in enumerate(valores.items()):
            with colunas[i]:
                html = f"""
                    <div style="text-align: left; margin-top: {margin_top}; margin-bottom: {margin_bottom}; line-height: 1.2;">
                        <span style="font-size:{fonte_label}; color:{cor_label}; white-space: nowrap;">{label}</span><br>
                        <b style="font-size:{fonte_valor}; color:{cor_valor}; white-space: nowrap;">{valor:,.0f}</b>
                    </div>
                """
                st.markdown(html, unsafe_allow_html=True)


# ==================================================
# Renderização do Dashboard em Tela Única (Full HD)
# ==================================================

# Cabeçalho Mina
st.markdown(f"""
    <div style="display: flex; justify-content: space-between; align-items: center;
        background-color: #2D3D70; padding: 0px 30px; border-radius: 8px; margin-top: 0.2px; margin-bottom: 0.2px;">
        <img src="data:{tipo_esquerda};base64,{base64_esquerda}" style="height: 45px;">
        <h1 style="color: white; font-size: 28px; margin: 0;">Performance Mina Paiol - Aura Almas</h1>
        <img src="data:{tipo_direita};base64,{base64_direita}" style="height: 40px;">
    </div>
""", unsafe_allow_html=True)

# Criação do Layout de cada linha
#=================================

# Linha 1 - Movimentação Total / Numero de Viagens
col1, col2 = st.columns([0.5, 0.5], gap="large")
with col1:
    valores_kpis = {
        "Acumulado": valor_mensal_viagens,
        "Ritmo Mês": ritmo_viagens,
        "Ontem": valor_ontem_viagens,
        "Hoje": valor_hoje_viagens,
        "Ritmo Dia": ritmo_viagens_dia,
        #"Meta dia": 710
    }

    exibir_kpis_customizados(
        valores=valores_kpis,
        imagem_base64=base64_kpi,
        imagem_tipo=tipo_kpi,
        cor_valor="#2D3D70",
        cor_label="#444",
        fonte_valor="22px",
        fonte_label="14px",
        altura_imagem="26px",
        margin_top="0px",
        margin_bottom="10px",
        padding_top_imagem="15px"
    )
    if not df_agg_viagens.empty:
        st.plotly_chart(grafico_numero_viagens.update_layout(height=270), use_container_width=True)

with col2:
    valores_kpis = {
        "Acumulado": valor_mensal_movimentacao_mina,
        "Ritmo Mês": ritmo_movimentacao,
        "Ontem": valor_ontem_movimentacao,
        "Hoje": valor_hoje_movimentacao,
        "Ritmo Dia": ritmo_movimentacao_dia,
        #"Meta dia": 71000
    }

    exibir_kpis_customizados(
        valores=valores_kpis,
        imagem_base64=base64_kpi,
        imagem_tipo=tipo_kpi,
        cor_valor="#2D3D70",
        cor_label="#444",
        fonte_valor="22px",
        fonte_label="14px",
        altura_imagem="26px",
        margin_top="0px",
        margin_bottom="10px",
        padding_top_imagem="15px"
    )
    if not df_agg_viagens.empty:
        st.plotly_chart(grafico_movimentacao_litogia.update_layout(height=270), use_container_width=True)

# Cabeçalho Moagem
st.markdown(f"""
    <div style="display: flex; justify-content: space-between; align-items: center;
        background-color: #2D3D70; padding: 0px 30px; border-radius: 8px; margin-top: 70px; margin-bottom: 5px;">
        <img src="data:{tipo_esquerda2};base64,{base64_esquerda2}" style="height: 40px;">
        <h1 style="color: white; font-size: 28px; margin: 0;">Performance Planta - Aura Almas</h1>
        <img src="data:{tipo_direita};base64,{base64_direita}" style="height: 40px;">
    </div>
""", unsafe_allow_html=True)

# Linha 2 - Alimentação Britagem / Alimentação Moagem
col3, col4 = st.columns([0.5, 0.5], gap="large")
with col3:
    valores_kpis = {
        "Acumulado": valor_mensal_britagem,
        "Ritmo Mês": ritmo_britagem,
        "Ontem": valor_ontem_britagem,
        "Hoje": valor_hoje_britagem,
        "Ritmo Dia": ritmo_britagem_dia,
        #"Meta dia": 71000
    }

    exibir_kpis_customizados(
        valores=valores_kpis,
        imagem_base64=base64_kpi,
        imagem_tipo=tipo_kpi,
        cor_valor="#2D3D70",
        cor_label="#444",
        fonte_valor="22px",
        fonte_label="14px",
        altura_imagem="26px",
        margin_top="0px",
        margin_bottom="10px",
        padding_top_imagem="15px"
    )
    if not df_agg_britagem.empty:
        st.plotly_chart(grafico_barra_britagem.update_layout(height=270), use_container_width=True)
with col4:
    valores_kpis = {
        "Acumulado": valor_mensal_moagem,
        "Ritmo Mês": ritmo_moagem,
        "Ontem": valor_ontem_moagem,
        "Hoje": valor_hoje_moagem,
        "Ritmo Dia": ritmo_moagem_dia,
        #"Meta dia": 71000
    }

    exibir_kpis_customizados(
        valores=valores_kpis,
        imagem_base64=base64_kpi,
        imagem_tipo=tipo_kpi,
        cor_valor="#2D3D70",
        cor_label="#444",
        fonte_valor="22px",
        fonte_label="14px",
        altura_imagem="26px",
        margin_top="0px",
        margin_bottom="10px",
        padding_top_imagem="15px"
    )
    if not df_agg_moagem.empty:
        st.plotly_chart(grafico_barra_moagem.update_layout(height=270), use_container_width=True)