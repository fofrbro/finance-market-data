"""
Extraction des cours boursiers quotidiens — couche bronze.

Récupère l'historique de dix valeurs et deux indices via yfinance,
contrôle la qualité des données et écrit un Parquet partitionné par
date d'extraction.

Principe : on ne transforme rien ici. La couche bronze conserve la
donnée telle que la source l'a fournie, avec juste de quoi tracer
d'où elle vient et quand elle est arrivée.

Usage :
    python extract_market_data.py
    python extract_market_data.py --start 2020-01-01 --out ./data
"""

import argparse
import logging
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf

# ---------------------------------------------------------------------
# Référentiel des instruments
# ---------------------------------------------------------------------
# Le calendrier de bourse et la devise sont portés ici parce qu'ils ne
# sont pas dans les données de cours, et qu'on en aura besoin en silver
# pour aligner les séries et convertir les montants.

INSTRUMENTS = [
    # ticker,     nom,          place,       devise, type
    ("AMZN",     "Amazon",      "NASDAQ",    "USD", "action"),
    ("TSLA",     "Tesla",       "NASDAQ",    "USD", "action"),
    ("MSFT",     "Microsoft",   "NASDAQ",    "USD", "action"),
    ("META",     "Meta",        "NASDAQ",    "USD", "action"),
    ("NVDA",     "Nvidia",      "NASDAQ",    "USD", "action"),
    ("ASML",     "ASML",        "NASDAQ",    "USD", "action"),
    ("BABA",     "Alibaba",     "NYSE",      "USD", "action"),
    ("1211.HK",  "BYD",         "HKEX",      "HKD", "action"),
    ("1810.HK",  "Xiaomi",      "HKEX",      "HKD", "action"),
    ("MC.PA",    "LVMH",        "Euronext",  "EUR", "action"),
    ("^GSPC",    "S&P 500",     "NYSE",      "USD", "indice"),
    ("^HSI",     "Hang Seng",   "HKEX",      "HKD", "indice"),
]

COLONNES_SOURCE = ["Open", "High", "Low", "Close", "Volume"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bronze")


# ---------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------

def telecharger(ticker: str, debut: str, fin: str) -> pd.DataFrame:
    """Télécharge l'historique d'un instrument.

    auto_adjust=False conserve la colonne 'Adj Close' séparément :
    on veut garder le prix brut ET le prix ajusté, pas l'un à la
    place de l'autre. L'ajustement sera fait explicitement en silver.
    """
    df = yf.download(
        ticker,
        start=debut,
        end=fin,
        interval="1d",
        auto_adjust=False,
        progress=False,
    )

    if df.empty:
        log.warning("%-9s aucune donnée retournée", ticker)
        return pd.DataFrame()

    # yfinance renvoie un MultiIndex de colonnes quand on passe une liste ;
    # on l'aplatit pour garder un schéma stable quel que soit l'appel.
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.reset_index()
    df = df.rename(columns={"Date": "date", "Adj Close": "adj_close"})
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    return df


def enrichir(df: pd.DataFrame, meta: tuple, extrait_le: datetime) -> pd.DataFrame:
    """Ajoute les métadonnées de traçabilité. Aucune valeur n'est modifiée."""
    ticker, nom, place, devise, type_ = meta
    df = df.copy()
    df["ticker"] = ticker
    df["nom"] = nom
    df["place"] = place
    df["devise"] = devise
    df["type_instrument"] = type_
    df["source"] = "yfinance"
    df["extrait_le"] = extrait_le
    return df


# ---------------------------------------------------------------------
# Contrôles qualité
# ---------------------------------------------------------------------

def controler(df: pd.DataFrame, ticker: str) -> list[str]:
    """Contrôles déterministes. Ne corrige rien : signale.

    En bronze on ne répare pas la donnée, on documente ses défauts.
    La correction appartient à la couche silver, où elle est tracée.
    """
    alertes = []

    if df.empty:
        return [f"{ticker}: dataframe vide"]

    manquantes = df[COLONNES_SOURCE + ["adj_close"]].isna().sum()
    for col, n in manquantes.items():
        if n:
            alertes.append(f"{ticker}: {n} valeurs manquantes sur '{col}'")

    n_doublons = df.duplicated(subset=["date"]).sum()
    if n_doublons:
        alertes.append(f"{ticker}: {n_doublons} dates en doublon")

    for col in ["open", "high", "low", "close", "adj_close"]:
        n = (df[col] <= 0).sum()
        if n:
            alertes.append(f"{ticker}: {n} prix nuls ou négatifs sur '{col}'")

    # Cohérence OHLC : le haut doit dominer, le bas doit être dominé.
    incoherent = ((df["high"] < df["low"]) |
                  (df["high"] < df["open"]) |
                  (df["high"] < df["close"]) |
                  (df["low"] > df["open"]) |
                  (df["low"] > df["close"])).sum()
    if incoherent:
        alertes.append(f"{ticker}: {incoherent} lignes OHLC incohérentes")

    # Trous supérieurs à une semaine : férié long, suspension de cotation,
    # ou lacune de la source. À regarder, pas à corriger automatiquement.
    ecarts = df["date"].sort_values().diff().dt.days
    longs = (ecarts > 7).sum()
    if longs:
        alertes.append(f"{ticker}: {longs} interruptions de plus de 7 jours")

    return alertes


# ---------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Extraction bronze des cours boursiers")
    parser.add_argument("--start", default="2015-01-01", help="date de début (AAAA-MM-JJ)")
    parser.add_argument("--end", default=None, help="date de fin exclue (défaut : aujourd'hui)")
    parser.add_argument("--out", default="./data/bronze", help="répertoire de sortie")
    args = parser.parse_args()

    fin = args.end or date.today().isoformat()
    extrait_le = datetime.now(timezone.utc)
    jour_extraction = extrait_le.date().isoformat()

    sortie = Path(args.out) / f"date_extraction={jour_extraction}"
    sortie.mkdir(parents=True, exist_ok=True)

    log.info("Extraction du %s au %s — %d instruments", args.start, fin, len(INSTRUMENTS))

    morceaux, toutes_alertes, echecs = [], [], []

    for meta in INSTRUMENTS:
        ticker = meta[0]
        try:
            df = telecharger(ticker, args.start, fin)
        except Exception as e:
            log.error("%-9s échec du téléchargement : %s", ticker, e)
            echecs.append(ticker)
            continue

        if df.empty:
            echecs.append(ticker)
            continue

        alertes = controler(df, ticker)
        toutes_alertes.extend(alertes)

        df = enrichir(df, meta, extrait_le)
        morceaux.append(df)

        log.info("%-9s %5d lignes   %s → %s",
                 ticker, len(df),
                 df["date"].min().date(), df["date"].max().date())

    if not morceaux:
        log.error("Aucune donnée extraite. Arrêt.")
        return

    complet = pd.concat(morceaux, ignore_index=True)
    chemin = sortie / "cours.parquet"
    complet.to_parquet(chemin, index=False, compression="snappy")

    # -----------------------------------------------------------------
    # Journal d'exécution — il accompagne les données et sera relu
    # en silver pour savoir à quoi s'attendre.
    # -----------------------------------------------------------------
    journal = sortie / "journal.txt"
    with journal.open("w", encoding="utf-8") as f:
        f.write(f"Extraction du {extrait_le.isoformat()}\n")
        f.write(f"Période demandée : {args.start} → {fin}\n")
        f.write(f"Instruments attendus : {len(INSTRUMENTS)}\n")
        f.write(f"Instruments extraits : {complet['ticker'].nunique()}\n")
        f.write(f"Lignes totales : {len(complet)}\n")
        if echecs:
            f.write(f"Échecs : {', '.join(echecs)}\n")
        f.write("\nLignes par instrument\n")
        for ticker, n in complet.groupby("ticker").size().items():
            f.write(f"  {ticker:<9} {n:>6}\n")
        f.write("\nAlertes qualité\n")
        if toutes_alertes:
            for a in toutes_alertes:
                f.write(f"  {a}\n")
        else:
            f.write("  aucune\n")

    log.info("Écrit : %s (%d lignes, %.1f Mo)",
             chemin, len(complet), chemin.stat().st_size / 1e6)
    log.info("Journal : %s", journal)

    if echecs:
        log.warning("Instruments en échec : %s", ", ".join(echecs))
    if toutes_alertes:
        log.warning("%d alertes qualité — voir le journal", len(toutes_alertes))


if __name__ == "__main__":
    main()
