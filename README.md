# Plateforme de données de marché — Microsoft Fabric

Pipeline de bout en bout qui ingère les cours de douze instruments financiers cotés sur trois places, les fiabilise et calcule des indicateurs de risque comparables entre eux.

**Stack :** Python · pandas · yfinance · PySpark · Delta Lake · Microsoft Fabric

---

## Le problème

Comparer la performance d'Amazon, de BYD et de LVMH n'a rien d'évident. Les trois cotent sur des places différentes, dans trois devises, selon trois calendriers de bourse qui ne se recouvrent pas. Un rendement brut en devise locale n'est pas comparable d'un instrument à l'autre.

Ce projet traite ces trois difficultés explicitement, plutôt que de les ignorer comme le font la plupart des analyses boursières en notebook.

## Architecture

```
yfinance (cours quotidiens)  ──┐
                               ├─→  BRONZE   Parquet brut, partitionné par date d'extraction
yfinance (taux de change)   ──┘              traçabilité : source, horodatage, devise
                                                  │
                                                  ▼
                                             SILVER   table Delta
                                                      valeurs manquantes reportées
                                                      rendements simples et logarithmiques
                                                      conversion en USD
                                                  │
                                                  ▼
                                             GOLD     table Delta
                                                      volatilité, Sharpe, drawdown
```

**Bronze** s'exécute en local (`extract_market_data.py`). **Silver** et **Gold** s'exécutent dans des notebooks PySpark sur Microsoft Fabric.

## Périmètre

| Ticker | Société | Place | Devise |
|---|---|---|---|
| AMZN, TSLA, MSFT, META, NVDA, ASML | valeurs technologiques US | NASDAQ | USD |
| BABA | Alibaba | NYSE | USD |
| 1211.HK, 1810.HK | BYD, Xiaomi | HKEX | HKD |
| MC.PA | LVMH | Euronext Paris | EUR |
| ^GSPC, ^HSI | S&P 500, Hang Seng | indices de référence | USD, HKD |

Historique du 2 janvier 2015 au 14 septembre 2026 — **34 296 lignes** dans
l'extraction de cours actuellement versionnée.

---

## Décisions techniques

### Le bronze ne corrige rien

Les contrôles qualité s'exécutent à l'ingestion mais ne modifient aucune valeur : ils écrivent un journal. La correction appartient au silver, où elle est tracée et réversible. C'est le principe de l'architecture médaillon — on doit toujours pouvoir revenir à la source.

### Pas de remplissage par zéro sur les jours non cotés

Réindexer sur le calendrier civil et remplir les week-ends par zéro produit une chute de 100 % le samedi et une hausse infinie le lundi. Le pipeline ne conserve que les jours de bourse réels, et reporte la dernière valeur connue (`last(..., ignorenulls=True)` sur une fenêtre cumulative) pour les rares trous.

### Le partitionnement des fenêtres

```python
w = Window.partitionBy("ticker").orderBy("date")
df.withColumn("close_veille", F.lag("close_ajuste").over(w))
```

Sans `partitionBy("ticker")`, la première cotation de Tesla serait comparée à la dernière d'Amazon. C'est l'erreur la plus fréquente sur les séries temporelles multi-entités.

### La jointure des taux de change

Le marché des changes cote 3 045 jours sur la période, les bourses entre 2 877 et 2 994. Une jointure exacte sur la date perdrait des lignes. Le pipeline fait une jointure à gauche puis reporte le dernier taux connu par fenêtre — le motif standard pour aligner deux sources de fréquences différentes.

### Une seule série de prix, explicitement ajustée

L'extraction demande `auto_adjust=False` pour obtenir le prix brut et le prix
ajusté séparément. Dans les faits, yfinance ne renvoie plus de colonne
`Adj Close` distincte : les cours retournés sont déjà ajustés des splits et
des dividendes. Nvidia apparaît ainsi à 1,69 $ en novembre 2016 alors qu'il
cotait autour de 85 $ à l'époque — deux splits sont intervenus depuis.

Conserver deux colonnes identiques en laissant croire qu'elles diffèrent serait
trompeur. Le pipeline n'en garde donc qu'une, renommée `close_ajuste` au silver
pour que la nature de la série soit explicite pour quiconque lit la table.

Cette ambiguïté de la source est la raison pour laquelle le bronze horodate et
trace chaque extraction : l'ajustement étant recalculé rétroactivement à chaque
distribution, deux extractions espacées de quelques mois ne renvoient pas les
mêmes valeurs historiques.

### Partitionnement des tables

`silver_cours` est partitionnée par ticker (filtre le plus fréquent). `gold_metriques` ne l'est pas : douze lignes ne se partitionnent pas. Partitionner une petite table dégrade les performances au lieu de les améliorer.

---

## Résultats

Rendements et volatilités convertis en USD, période 2015-2026.

| Instrument | Sharpe | Drawdown max |
|---|---|---|
| Nvidia | 1,26 | −66 % |
| Amazon | 0,81 | −56 % |
| Microsoft | 0,79 | −38 % |
| ASML | 0,73 | −57 % |
| Tesla | 0,72 | −74 % |
| Meta | 0,61 | −77 % |
| **S&P 500** | **0,58** | **−34 %** |
| BYD | 0,55 | −58 % |
| LVMH | 0,38 | −55 % |
| Xiaomi | 0,31 | −76 % |
| Alibaba | 0,16 | −80 % |
| Hang Seng | 0,01 | −56 % |

### Ce que ces chiffres disent

**L'indice est difficile à battre.** Seules six valeurs sur onze dépassent le S&P 500 en rendement ajusté du risque, et toutes le font avec un drawdown nettement pire. Le S&P affiche le meilleur drawdown de tout l'échantillon.

**La surperformance se paie en drawdown, pas en volatilité.** Nvidia affiche le meilleur Sharpe et une perte maximale de 66 % — 10 000 € devenus 3 400 € au creux de 2022. C'est cette expérience-là qui fait vendre au pire moment, et la volatilité ne la capture pas.

**Alibaba est le contre-exemple utile.** Sharpe de 0,16 pour un drawdown de 80 % : dix ans de risque maximal pour un rendement à peine supérieur au taux sans risque.

**L'effet devise change le classement.** Le Sharpe de LVMH passe de 0,398 en euros à 0,382 en dollars. L'écart paraît faible sur un rendement quotidien ; cumulé sur dix ans, il représente plusieurs points de performance. Toute comparaison internationale qui ignore le change est biaisée.

### Contrôles qualité

Trois variations supérieures à 25 % en une séance ont été signalées automatiquement, puis vérifiées manuellement :

| Date | Instrument | Variation | Cause |
|---|---|---|---|
| 2016-11-11 | Nvidia | +30 % | résultats trimestriels très au-dessus des attentes |
| 2022-02-03 | Meta | −26 % | première baisse d'utilisateurs quotidiens annoncée |
| 2022-03-16 | Alibaba | +37 % | annonce de soutien de Pékin aux valeurs tech chinoises |

Aucune n'est une erreur de données. Le pipeline signale, l'humain tranche — une règle automatique de suppression des valeurs extrêmes aurait effacé trois faits de marché réels.

---

## Limites

**Ces indicateurs sont rétrospectifs.** Un Sharpe de 1,26 sur 2015-2026 ne dit rien de 2027. Ils servent à dimensionner un risque, pas à prédire un rendement.

**Xiaomi démarre en juillet 2018**, date de son introduction en bourse, contre janvier 2015 pour les autres. Toute corrélation calculée sans restreindre aux dates communes serait faussée. Le champ `premiere_cotation` rend l'écart explicite.

**Pas de prise en compte des dividendes versés en espèces** au-delà de l'ajustement appliqué par la source.

**Le taux sans risque est fixé à 2,5 %** sur toute la période, alors qu'il a varié de façon importante entre 2015 et 2026.

---

## Contenu du dépôt

```
extract_market_data.py             extraction bronze, exécutable en ligne de commande
Donnees_boursieres.ipynb           exploration de l'extraction des cours
TauxConvert.ipynb                  extraction des taux de change
Bourse.Notebook/                   notebook Fabric Silver et Gold
Finance.Lakehouse/                 définition du Lakehouse Fabric
referentiel_devises.csv            référentiel des devises
```

## Reproduire

```bash
conda create -n finance python=3.11 -y
conda activate finance
conda install pandas pyarrow jupyter -y
pip install yfinance

python extract_market_data.py --start 2015-01-01
```

Puis téléverser `data/bronze/` et `data/bronze_fx/` dans la section Files du
Lakehouse Fabric. Ouvrir ensuite `Bourse.Notebook/notebook-content.py` dans
Fabric et exécuter les cellules dans l'ordre : le même notebook construit
`silver_cours`, calcule les métriques intermédiaires, joint les taux de change,
puis écrit `gold_metriques`.

---

## Suite

- Ingestion temps réel de ticks intrajournaliers (Eventstream → Eventhouse), détection d'anomalies de prix en KQL
- Matrice de corrélation sur dates communes et frontière efficiente
- Restitution Power BI en Direct Lake
- `OPTIMIZE` et Z-Order sur les tables Delta
