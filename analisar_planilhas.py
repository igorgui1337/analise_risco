#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Analisador em Lote de Planilhas de Risco (com Top_Jogadores)
- Unifica .xlsx de uma pasta
- Converte R$ e % para número
- Calcula correlações e estatísticas
- Gera gráficos e Excel consolidado
- NOVO: Cria aba Top_Jogadores (alto risco)
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

# ===== NOVO: constrói Top_Jogadores
def build_top_players(df: pd.DataFrame, top_percent_volume: float = 0.95,
                      min_winrate: float = 70.0, only_player_profit: bool = True,
                      min_qtd_apostas: int = 10, top_n: int = 200) -> pd.DataFrame:
    cols_need = ['user_id','win_rate_5h','qtd_apostas_5h','volume_apostado_5h',
                 'lucro_perda_5h','net_deposit_7d','__arquivo__']
    cols = [c for c in cols_need if c in df.columns]
    base = df[cols].copy()

    # limites
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

    # ranking
    sort_cols = []
    if 'win_rate_5h' in top: sort_cols.append(('win_rate_5h', False))
    if 'volume_apostado_5h' in top: sort_cols.append(('volume_apostado_5h', False))
    if 'lucro_perda_5h' in top: sort_cols.append(('lucro_perda_5h', False))

    if sort_cols:
        top = top.sort_values(by=[c for c, _ in sort_cols],
                              ascending=[asc for _, asc in sort_cols])

    # métricas auxiliares
    if {'volume_apostado_5h','lucro_perda_5h'}.issubset(top.columns):
        top['roi_5h_%'] = np.where(top['volume_apostado_5h']>0,
                                   100*top['lucro_perda_5h']/top['volume_apostado_5h'], 0)

    # manter top_n
    if len(top) > top_n:
        top = top.head(top_n)

    # reordenar colunas
    final_order = [c for c in ['user_id','win_rate_5h','qtd_apostas_5h','volume_apostado_5h',
                               'lucro_perda_5h','roi_5h_%','net_deposit_7d','__arquivo__']
                   if c in top.columns]
    return top[final_order] if final_order else top

# =========================
# Main
# =========================

def main():
    parser = argparse.ArgumentParser(description="Unificar e analisar múltiplas planilhas de risco (.xlsx)")
    parser.add_argument("--pasta", required=True, help="Caminho da pasta contendo os .xlsx")
    parser.add_argument("--aba", default=None, help="Nome da aba (sheet) a ler em cada arquivo (opcional)")
    # parâmetros de Top_Jogadores (ajuste se quiser)
    parser.add_argument("--min_winrate", type=float, default=70.0)
    parser.add_argument("--perc_volume", type=float, default=0.95, help="Percentil de volume (0-1) para filtrar alto volume")
    parser.add_argument("--min_apostas", type=int, default=10)
    parser.add_argument("--top_n", type=int, default=200)
    parser.add_argument("--somente_lucro_jogador", action="store_true", help="Filtra apenas quem teve lucro (lucro_perda_5h > 0)")

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

    # ===== NOVO: construir Top_Jogadores
    top_players = build_top_players(
        base,
        top_percent_volume=args.perc_volume,
        min_winrate=args.min_winrate,
        only_player_profit=args.somente_lucro_jogador,
        min_qtd_apostas=args.min_apostas,
        top_n=args.top_n
    )

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    relatorio_path = os.path.join("saidas", f"relatorio_correlacoes_{ts}.xlsx")

    with pd.ExcelWriter(relatorio_path, engine="openpyxl") as writer:
        base.to_excel(writer, sheet_name="Base Unificada", index=False)
        if not pearson.empty:
            pearson.to_excel(writer, sheet_name="Correlacao_Pearson")
        if not spearman.empty:
            spearman.to_excel(writer, sheet_name="Correlacao_Spearman")
        descr.to_excel(writer, sheet_name="Estatisticas")
        # ===== NOVO: gravar Top_Jogadores
        if not top_players.empty:
            top_players.to_excel(writer, sheet_name="Top_Jogadores", index=False)

    print(f"OK! Relatório salvo em: {relatorio_path}")
    print("Imagens geradas em: ./saidas/ (dispersões e heatmaps)")
    if not top_players.empty:
        print(f"Top_Jogadores: {len(top_players)} registros (aba criada).")
    else:
        print("Top_Jogadores: sem registros para os critérios definidos.")

if __name__ == "__main__":
    main()
