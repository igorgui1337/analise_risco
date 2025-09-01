#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Analisador em Lote de Planilhas de Risco
- Unifica .xlsx de uma pasta
- Converte R$ e % para número
- Calcula correlações e estatísticas
- Gera gráficos (dispersões, heatmaps)
- Abas no Excel:
  * Base Unificada
  * Correlacao_Pearson
  * Correlacao_Spearman
  * Estatisticas
  * Top_Jogadores (alto risco)
  * Top_Jogos (títulos com maior ROI/volume)
  * Temporal_Resumo  <-- NOVO
"""

import os
import sys
import argparse
import glob
from datetime import datetime
import warnings

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns


warnings.filterwarnings("ignore")

# =========================
# Utilitários
# =========================

def parse_monetary_value(value):
    if pd.isna(value) or value in ('N/A', '', None):
        return 0.0
    if isinstance(value, (int, float, np.integer, np.floating)):
        return float(value)
    s = str(value).replace('R$', '').replace('.', '').replace(',', '.').strip()
    try:
        return float(s)
    except Exception:
        return 0.0

def parse_percentage(value):
    if pd.isna(value) or value in ('', None):
        return 0.0
    if isinstance(value, (int, float, np.integer, np.floating)):
        return float(value)
    s = str(value).replace('%', '').replace(',', '.').strip()
    try:
        return float(s)
    except Exception:
        return 0.0

COLUMN_MAPPING = {
    'Status Atual': 'status',
    'ID Usuário': 'user_id',
    'Saldo (R$)': 'saldo',
    'Data de Cadastro': 'data_cadastro',
    'Depósitos Totais (7d)': 'depositos_7d',
    'Saques Totais (7d)': 'saques_7d',
    'Net Deposit (7d)': 'net_deposit_7d',
    'Qtd de Apostas (5h)': 'qtd_apostas_5h',
    'Volume Total Apostado (5h)': 'volume_apostado_5h',
    'Ticket Médio (5h)': 'ticket_medio_5h',
    'Aposta Máxima (5h)': 'aposta_max_5h',
    'Aposta Mínima (5h)': 'aposta_min_5h',
    'Volume Total Retornado (5h)': 'volume_retornado_5h',
    'Jogo Mais Jogado': 'jogo_mais_jogado',
    'Qtd Greens (5h)': 'qtd_greens_5h',
    'Qtd Reds (5h)': 'qtd_reds_5h',
    'Win Rate % (5h)': 'win_rate_5h',
    'Lucro/Perda Líquido (5h)': 'lucro_perda_5h',
    'Qtd Saques por Dia (máx nos últimos 7d)': 'qtd_saques_dia',
    # variações
    'ID Usuario': 'user_id',
    'Saldo': 'saldo',
    'data do cadastro': 'data_cadastro',
    'deposito (7d)': 'depositos_7d',
    'saques totais (7d)': 'saques_7d',
    'Net Deposito': 'net_deposit_7d',
    'QTD apostas 5h': 'qtd_apostas_5h',
    'Volume Total Apostado 5h': 'volume_apostado_5h',
    'Tick Medio 5h': 'ticket_medio_5h',
    'Aposta Maxima 5h': 'aposta_max_5h',
    'Aposta Minima 5h': 'aposta_min_5h',
    'Volume Retornado 5h': 'volume_retornado_5h',
    'Jogo mais jogado': 'jogo_mais_jogado',
    'Qnt Greens 5h': 'qtd_greens_5h',
    'Qtd Reds 5h': 'qtd_reds_5h',
    'Win Rate % 5h': 'win_rate_5h',
    'Lucro/Perda Liquido 5h': 'lucro_perda_5h',
    'qts saques por dia': 'qtd_saques_dia'
}

MONETARY_COLS = [
    'saldo','depositos_7d','saques_7d','net_deposit_7d',
    'volume_apostado_5h','ticket_medio_5h','aposta_max_5h',
    'aposta_min_5h','volume_retornado_5h','lucro_perda_5h'
]

NUMERIC_COLS = ['qtd_apostas_5h','qtd_greens_5h','qtd_reds_5h','qtd_saques_dia']

# =========================
# Núcleo
# =========================

def read_one_excel(path, sheet=None):
    try:
        if sheet is None:
            xls = pd.ExcelFile(path)
            candidate = None
            for s in xls.sheet_names:
                df_try = pd.read_excel(path, sheet_name=s)
                if df_try.shape[1] >= 5 and len(df_try) >= 1:
                    candidate = s
                    break
            sheet_to_read = candidate if candidate else xls.sheet_names[0]
        else:
            sheet_to_read = sheet

        df = pd.read_excel(path, sheet_name=sheet_to_read)
        df = df.rename(columns=COLUMN_MAPPING)
        df['__arquivo__'] = os.path.basename(path)
        return df
    except Exception as e:
        print(f"[ERRO] Falha ao ler '{path}': {e}")
        return None

def standardize_types(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in MONETARY_COLS:
        if col in df.columns:
            df[col] = df[col].apply(parse_monetary_value)
    if 'win_rate_5h' in df.columns:
        df['win_rate_5h'] = df['win_rate_5h'].apply(parse_percentage)
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    # datas
    if 'data_cadastro' in df.columns:
        df['data_cadastro'] = pd.to_datetime(df['data_cadastro'], errors='coerce')
    return df

# ===== NOVO: análise temporal
def add_temporal_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """Adiciona análise de padrões temporais no DataFrame."""
    if 'data_cadastro' in df.columns:
        # já convertido em standardize_types
        now = datetime.now()
        df['dias_desde_cadastro'] = (now - df['data_cadastro']).dt.days
        df['dias_desde_cadastro'] = df['dias_desde_cadastro'].fillna(-1).astype(int)

        if 'volume_apostado_5h' in df.columns:
            df['velocidade_apostas'] = df['volume_apostado_5h'] / (df['dias_desde_cadastro'].clip(lower=0) + 1)

        # Flag contas novas com alto volume (quantil 80 do volume)
        vol_q80 = df['volume_apostado_5h'].quantile(0.80) if 'volume_apostado_5h' in df.columns else np.nan
        df['conta_nova_alto_risco'] = (
            (df['dias_desde_cadastro'] >= 0) &
            (df['dias_desde_cadastro'] < 7) &
            (('volume_apostado_5h' in df.columns) & (df['volume_apostado_5h'] > vol_q80))
        )
    return df

def make_correlations(df: pd.DataFrame):
    candidates = [
        'qtd_greens_5h','qtd_reds_5h','win_rate_5h','aposta_max_5h','aposta_min_5h',
        'volume_apostado_5h','volume_retornado_5h','lucro_perda_5h','net_deposit_7d',
        'risk_score','qtd_apostas_5h'
    ]
    cols = [c for c in candidates if c in df.columns]
    if not cols:
        return pd.DataFrame(), pd.DataFrame()
    num = df[cols].apply(pd.to_numeric, errors='coerce')
    pearson = num.corr(method='pearson')
    spearman = num.corr(method='spearman')
    return pearson, spearman

def save_heatmap(corr: pd.DataFrame, title: str, outpath: str):
    if corr.empty:
        return
    plt.figure(figsize=(10, 8))
    plt.imshow(corr, interpolation='nearest')
    plt.title(title)
    plt.xticks(range(len(corr.columns)), corr.columns, rotation=45, ha='right')
    plt.yticks(range(len(corr.index)), corr.index)
    plt.colorbar()
    for i in range(corr.shape[0]):
        for j in range(corr.shape[1]):
            val = corr.values[i, j]
            plt.text(j, i, f"{val:.2f}", va='center', ha='center', fontsize=8)
    plt.tight_layout()
    plt.savefig(outpath, dpi=200)
    plt.close()

# ===== Top_Jogadores
def build_top_players(df: pd.DataFrame, top_percent_volume: float = 0.95,
                      min_winrate: float = 70.0, only_player_profit: bool = True,
                      min_qtd_apostas: int = 10, top_n: int = 200) -> pd.DataFrame:
    cols_need = ['user_id','win_rate_5h','qtd_apostas_5h','volume_apostado_5h',
                 'lucro_perda_5h','net_deposit_7d','__arquivo__',
                 'conta_nova_alto_risco','dias_desde_cadastro','velocidade_apostas']
    cols = [c for c in cols_need if c in df.columns]
    base = df[cols].copy()

    vol_thr = base['volume_apostado_5h'].quantile(top_percent_volume) if 'volume_apostado_5h' in base else 0
    conds = pd.Series(True, index=base.index)

    if 'win_rate_5h' in base:
        conds &= base['win_rate_5h'] >= min_winrate
    if 'volume_apostado_5h' in base:
        conds &= base['volume_apostado_5h'] >= vol_thr
    if 'lucro_perda_5h' in base and only_player_profit:
        conds &= base['lucro_perda_5h'] > 0
    if 'qtd_apostas_5h' in base:
        conds &= base['qtd_apostas_5h'] >= min_qtd_apostas

    top = base[conds].copy()

    # ROI auxiliar
    if {'volume_apostado_5h','lucro_perda_5h'}.issubset(top.columns):
        top['roi_5h_%'] = np.where(top['volume_apostado_5h']>0,
                                   100*top['lucro_perda_5h']/top['volume_apostado_5h'], 0)

    # Ranking: prioriza contas novas alto risco primeiro
    sort_cols = []
    if 'conta_nova_alto_risco' in top: sort_cols.append(('conta_nova_alto_risco', False))  # True > False
    if 'win_rate_5h' in top: sort_cols.append(('win_rate_5h', False))
    if 'volume_apostado_5h' in top: sort_cols.append(('volume_apostado_5h', False))
    if 'lucro_perda_5h' in top: sort_cols.append(('lucro_perda_5h', False))
    if sort_cols:
        top = top.sort_values(by=[c for c,_ in sort_cols], ascending=[asc for _,asc in sort_cols])

    if len(top) > top_n:
        top = top.head(top_n)

    final_order = [c for c in [
        'user_id','conta_nova_alto_risco','dias_desde_cadastro','velocidade_apostas',
        'win_rate_5h','qtd_apostas_5h','volume_apostado_5h',
        'lucro_perda_5h','roi_5h_%','net_deposit_7d','__arquivo__'
    ] if c in top.columns]
    return top[final_order] if final_order else top

# ===== Top_Jogos
def build_top_games(df: pd.DataFrame, perc_volume_game: float = 0.95,
                    min_distinct_players: int = 10, top_n: int = 200) -> pd.DataFrame:
    req = {'jogo_mais_jogado','volume_apostado_5h','volume_retornado_5h','lucro_perda_5h','qtd_apostas_5h','user_id'}
    if not req.issubset(df.columns):
        return pd.DataFrame()

    grp = df.groupby('jogo_mais_jogado', dropna=False).agg(
        volume_total=('volume_apostado_5h', 'sum'),
        retorno_total=('volume_retornado_5h', 'sum'),
        lucro_jogadores=('lucro_perda_5h', 'sum'),
        apostas_totais=('qtd_apostas_5h', 'sum'),
        jogadores_distintos=('user_id', 'nunique')
    ).reset_index()

    grp['roi_%'] = np.where(grp['volume_total']>0, 100*grp['lucro_jogadores']/grp['volume_total'], 0)

    vol_thr = grp['volume_total'].quantile(perc_volume_game) if len(grp) else 0
    mask = (grp['volume_total'] >= vol_thr) & (grp['jogadores_distintos'] >= min_distinct_players)
    top_games = grp.loc[mask].copy()

    top_games = top_games.sort_values(by=['roi_%','volume_total'], ascending=[False, False])

    if len(top_games) > top_n:
        top_games = top_games.head(top_n)

    cols = ['jogo_mais_jogado','roi_%','volume_total','retorno_total','lucro_jogadores','apostas_totais','jogadores_distintos']
    return top_games[cols]

# ===== Resumo temporal para Excel
def build_temporal_summary(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    # Estatísticas básicas
    if 'dias_desde_cadastro' in df.columns:
        dias = df['dias_desde_cadastro'].replace({-1: np.nan})
        rows.append({'metric': 'dias_desde_cadastro_média', 'value': dias.mean()})
        rows.append({'metric': 'dias_desde_cadastro_mediana', 'value': dias.median()})
    if 'velocidade_apostas' in df.columns:
        vel = df['velocidade_apostas']
        for q in [0.25, 0.5, 0.75, 0.90]:
            rows.append({'metric': f'velocidade_apostas_q{int(q*100)}', 'value': vel.quantile(q)})
    if 'conta_nova_alto_risco' in df.columns:
        pct = 100*df['conta_nova_alto_risco'].mean()
        rows.append({'metric': 'pct_contas_novas_alto_risco', 'value': pct})

    # Distribuição por faixas de idade
    if 'dias_desde_cadastro' in df.columns:
        bins = [-1, 0, 7, 30, 90, 180, 365, 99999]
        labels = ['sem_data','0d','1-7d','8-30d','31-90d','91-180d','>180d']
        cut = pd.cut(df['dias_desde_cadastro'], bins=bins, labels=labels, right=True)
        dist = cut.value_counts(dropna=False).rename_axis('faixa_dias').reset_index(name='qtd')
        dist['metric'] = 'distribuicao_faixas_dias'
        rows.extend(dist[['metric','faixa_dias','qtd']].to_dict('records'))

    out = pd.DataFrame(rows)
    return out
# =========================
# Novos gráficos
# =========================

def plot_profit_by_winrate_bins(df: pd.DataFrame, outpath: str = "saidas/boxplot_lucro_winrate.png"):
    """Boxplot de lucro/perda por faixas de win rate."""
    if 'win_rate_5h' not in df.columns or 'lucro_perda_5h' not in df.columns:
        return
    df = df.copy()
    # garante numérico
    df['win_rate_5h'] = pd.to_numeric(df['win_rate_5h'], errors='coerce')
    df['lucro_perda_5h'] = pd.to_numeric(df['lucro_perda_5h'], errors='coerce')
    # cria bins
    df['winrate_bin'] = pd.cut(
        df['win_rate_5h'],
        bins=[0, 30, 50, 70, 100],
        labels=['Baixo', 'Médio', 'Alto', 'Suspeito'],
        include_lowest=True
    )
    # remove NaN
    df = df.dropna(subset=['winrate_bin', 'lucro_perda_5h'])
    if df.empty:
        return

    plt.figure(figsize=(8,6))
    df.boxplot(column='lucro_perda_5h', by='winrate_bin')
    plt.title('Distribuição de Lucros por Faixa de Win Rate')
    plt.suptitle('')
    plt.xlabel('Faixa de Win Rate')
    plt.ylabel('Lucro/Perda (R$)')
    plt.tight_layout()
    plt.savefig(outpath, dpi=180)
    plt.close()


def plot_temporal_heatmap(df: pd.DataFrame, outpath: str = "saidas/heatmap_temporal.png"):
    """Heatmap de lucro/perda por mês de cadastro vs. faixa de win rate."""
    req_cols = {'data_cadastro','win_rate_5h','lucro_perda_5h'}
    if not req_cols.issubset(df.columns):
        return

    df = df.copy()
    df['data_cadastro'] = pd.to_datetime(df['data_cadastro'], errors='coerce')
    df['win_rate_5h'] = pd.to_numeric(df['win_rate_5h'], errors='coerce')
    df['lucro_perda_5h'] = pd.to_numeric(df['lucro_perda_5h'], errors='coerce')

    # bins (mesmos do boxplot)
    df['winrate_bin'] = pd.cut(
        df['win_rate_5h'],
        bins=[0, 30, 50, 70, 100],
        labels=['Baixo', 'Médio', 'Alto', 'Suspeito'],
        include_lowest=True
    )

    df = df.dropna(subset=['data_cadastro','winrate_bin','lucro_perda_5h'])
    if df.empty:
        return

    df['mes_cadastro'] = df['data_cadastro'].dt.to_period('M').astype(str)

    pivot = df.pivot_table(
        values='lucro_perda_5h',
        index='mes_cadastro',
        columns='winrate_bin',
        aggfunc='sum',
        fill_value=0
    )

    plt.figure(figsize=(9,6))
    ax = sns.heatmap(pivot, annot=True, fmt='.0f', cmap='RdYlGn_r')
    ax.set_xlabel('Faixa de Win Rate')
    ax.set_ylabel('Mês de Cadastro')
    plt.title('Lucro/Perda por Mês de Cadastro e Faixa de Win Rate')
    plt.tight_layout()
    plt.savefig(outpath, dpi=180)
    plt.close()
# =========================
# Main
# =========================

def main():
    parser = argparse.ArgumentParser(description="Unificar e analisar múltiplas planilhas de risco (.xlsx)")
    parser.add_argument("--pasta", required=True, help="Caminho da pasta contendo os .xlsx")
    parser.add_argument("--aba", default=None, help="Nome da aba (sheet) a ler em cada arquivo (opcional)")

    # Top_Jogadores
    parser.add_argument("--min_winrate", type=float, default=70.0)
    parser.add_argument("--perc_volume", type=float, default=0.95, help="Percentil de volume (0-1) para filtrar alto volume (jogadores)")
    parser.add_argument("--min_apostas", type=int, default=10)
    parser.add_argument("--top_n", type=int, default=200)
    parser.add_argument("--somente_lucro_jogador", action="store_true", help="Filtra apenas quem teve lucro (lucro_perda_5h > 0)")

    # Top_Jogos
    parser.add_argument("--perc_volume_jogo", type=float, default=0.95, help="Percentil de volume por jogo para entrar no ranking")
    parser.add_argument("--min_jogadores_jogo", type=int, default=10, help="Mínimo de jogadores distintos por jogo")

    args = parser.parse_args()

    pasta = args.pasta
    sheet = args.aba
    arquivos = sorted(glob.glob(os.path.join(pasta, "*.xlsx")))
    if not arquivos:
        print(f"[ERRO] Nenhum .xlsx encontrado em: {pasta}")
        sys.exit(1)

    print(f"Encontrados {len(arquivos)} arquivos .xlsx. Lendo...")
    dfs = []
    for path in arquivos:
        df = read_one_excel(path, sheet=sheet)
        if df is None or df.empty:
            continue
        df = standardize_types(df)
        df = add_temporal_analysis(df)   # <<< NOVO no pipeline
        dfs.append(df)

    if not dfs:
        print("[ERRO] Nenhum arquivo pôde ser lido com sucesso.")
        sys.exit(1)

    base = pd.concat(dfs, ignore_index=True)
    print(f"Base unificada: {base.shape[0]:,} linhas x {base.shape[1]} colunas")

    descr = base.describe(include='all').fillna("")
    pearson, spearman = make_correlations(base)

    os.makedirs("saidas", exist_ok=True)

    # Dispersões padrão
    def scatter(x, y, fname):
        if x in base.columns and y in base.columns:
            plt.figure()
            plt.scatter(base[x], base[y], alpha=0.3)
            plt.xlabel(x); plt.ylabel(y)
            plt.title(f"Dispersão: {x} vs {y}")
            plt.tight_layout()
            plt.savefig(os.path.join("saidas", fname), dpi=180)
            plt.close()

    scatter("qtd_greens_5h", "qtd_reds_5h", "disp_greens_vs_reds.png")
    scatter("qtd_greens_5h", "win_rate_5h", "disp_greens_vs_winrate.png")
    scatter("volume_apostado_5h", "lucro_perda_5h", "disp_volume_vs_lucro.png")

    save_heatmap(pearson, "Correlação de Pearson", os.path.join("saidas", "heatmap_pearson.png"))
    save_heatmap(spearman, "Correlação de Spearman", os.path.join("saidas", "heatmap_spearman.png"))
   
    # Gráficos de dispersão e heatmaps já existentes...
    scatter("qtd_greens_5h", "qtd_reds_5h", "disp_greens_vs_reds.png")
    scatter("qtd_greens_5h", "win_rate_5h", "disp_greens_vs_winrate.png")
    scatter("volume_apostado_5h", "lucro_perda_5h", "disp_volume_vs_lucro.png")

    save_heatmap(pearson, "Correlação de Pearson", os.path.join("saidas", "heatmap_pearson.png"))
    save_heatmap(spearman, "Correlação de Spearman", os.path.join("saidas", "heatmap_spearman.png"))

    # >>> NOVOS GRÁFICOS
    plot_profit_by_winrate_bins(base, outpath=os.path.join("saidas","boxplot_lucro_winrate.png"))
    plot_temporal_heatmap(base, outpath=os.path.join("saidas","heatmap_temporal.png"))

    # Rankings
    top_players = build_top_players(
        base,
        top_percent_volume=args.perc_volume,
        min_winrate=args.min_winrate,
        only_player_profit=args.somente_lucro_jogador,
        min_qtd_apostas=args.min_apostas,
        top_n=args.top_n
    )

    top_games = build_top_games(
        base,
        perc_volume_game=args.perc_volume_jogo,
        min_distinct_players=args.min_jogadores_jogo,
        top_n=args.top_n
    )

    # Resumo temporal
    temporal_summary = build_temporal_summary(base)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    relatorio_path = os.path.join("saidas", f"relatorio_correlacoes_{ts}.xlsx")

    # Tabela auxiliar de bins de win rate
    win_bins_tbl = None
    if 'win_rate_5h' in base.columns:
        tmp = base[['win_rate_5h']].copy()
        tmp['winrate_bin'] = pd.cut(
            pd.to_numeric(tmp['win_rate_5h'], errors='coerce'),
            bins=[0, 30, 50, 70, 100],
            labels=['Baixo', 'Médio', 'Alto', 'Suspeito'],
            include_lowest=True
        )
        win_bins_tbl = tmp['winrate_bin'].value_counts(dropna=False).rename_axis('faixa').reset_index(name='qtd')


    with pd.ExcelWriter(relatorio_path, engine="openpyxl") as writer:
        base.to_excel(writer, sheet_name="Base Unificada", index=False)
        if not pearson.empty:
            pearson.to_excel(writer, sheet_name="Correlacao_Pearson")
        if not spearman.empty:
            spearman.to_excel(writer, sheet_name="Correlacao_Spearman")
        descr.to_excel(writer, sheet_name="Estatisticas")
        if not top_players.empty:
            top_players.to_excel(writer, sheet_name="Top_Jogadores", index=False)
        if not top_games.empty:
            top_games.to_excel(writer, sheet_name="Top_Jogos", index=False)
        if not temporal_summary.empty:
            temporal_summary.to_excel(writer, sheet_name="Temporal_Resumo", index=False)
        if win_bins_tbl is not None:
            win_bins_tbl.to_excel(writer, sheet_name="WinRate_Bins", index=False)


    print(f"OK! Relatório salvo em: {relatorio_path}")
    print("Imagens geradas em: ./saidas/ (dispersões e heatmaps)")
    if not top_players.empty:
        print(f"Top_Jogadores: {len(top_players)} registros.")
    if not top_games.empty:
        print(f"Top_Jogos: {len(top_games)} títulos.")
    if not temporal_summary.empty:
        print("Temporal_Resumo: gerado.")

if __name__ == "__main__":
    main()
