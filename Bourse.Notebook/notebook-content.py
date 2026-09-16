# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "43630661-3f6c-4806-8ff4-32474588fd0f",
# META       "default_lakehouse_name": "Finance",
# META       "default_lakehouse_workspace_id": "6ba9ea4f-497e-4523-be3f-90d070c024ca",
# META       "known_lakehouses": [
# META         {
# META           "id": "43630661-3f6c-4806-8ff4-32474588fd0f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.read.parquet("Files/bronze/**/*.parquet")
print(df.count(), len(df.columns))
df.printSchema()
df.groupBy("ticker").count().orderBy("ticker").show(20)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.parquet("Files/bronze")
df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

derniere = df.agg(F.max("date_extraction")).collect()[0][0]
print("Dernière extraction :", derniere)

bronze_recent = df.filter(F.col("date_extraction") == derniere)
print(bronze_recent.count(), "lignes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.window import Window

silver = (bronze_recent
    .withColumn("date", F.to_date("date"))
    .drop("adj_close", "source")
    .withColumnRenamed("close", "close_ajuste")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

w_ffill = (Window.partitionBy("ticker").orderBy("date")
           .rowsBetween(Window.unboundedPreceding, 0))

silver = silver.withColumn(
    "close_ajuste",
    F.last("close_ajuste", ignorenulls=True).over(w_ffill)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

w = Window.partitionBy("ticker").orderBy("date")

silver = (silver
    .withColumn("close_veille", F.lag("close_ajuste").over(w))
    .withColumn("rendement",
        (F.col("close_ajuste") - F.col("close_veille")) / F.col("close_veille"))
    .withColumn("rendement_log",
        F.log(F.col("close_ajuste") / F.col("close_veille")))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

w_all = Window.partitionBy("ticker")

silver = (silver
    .withColumn("premiere_cotation", F.min("date").over(w_all))
    .withColumn("jours_historique",
        F.datediff(F.col("date"), F.col("premiere_cotation")))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

controles = silver.agg(
    F.count("*").alias("lignes"),
    F.sum(F.col("close_ajuste").isNull().cast("int")).alias("prix_nuls"),
    F.sum((F.abs(F.col("rendement")) > 0.25).cast("int")).alias("variations_extremes"),
    F.countDistinct("ticker").alias("instruments")
)
display(controles)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("ticker")
    .saveAsTable("silver_cours"))

display(spark.sql("DESCRIBE DETAIL silver_cours"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(silver.filter(F.abs("rendement") > 0.25)
              .select("date", "ticker", "close_veille", "close_ajuste", "rendement")
              .orderBy("date"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

TAUX_SANS_RISQUE = 0.025   # 2,5 % annuel
JOURS_BOURSE = 252

base = silver_fx.filter(F.col("rendement_usd").isNotNull())

metriques = (base.groupBy("ticker", "nom", "place", "devise")
    .agg(
        F.count("*").alias("nb_seances"),
        F.min("date").alias("debut"),
        F.max("date").alias("fin"),
        F.avg("rendement_usd").alias("rendement_moyen_jour"),
        F.stddev("rendement_usd").alias("volatilite_jour"),
    )
    .withColumn("rendement_annuel", F.col("rendement_moyen_jour") * JOURS_BOURSE)
    .withColumn("volatilite_annuelle", F.col("volatilite_jour") * F.sqrt(F.lit(JOURS_BOURSE)))
    .withColumn("sharpe",
        (F.col("rendement_annuel") - F.lit(TAUX_SANS_RISQUE)) / F.col("volatilite_annuelle"))
)

display(metriques.orderBy(F.desc("sharpe")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

w = Window.partitionBy("ticker").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)

dd = (silver
    .withColumn("plus_haut", F.max("close_ajuste").over(w))
    .withColumn("drawdown", F.col("close_ajuste") / F.col("plus_haut") - 1)
    .groupBy("ticker")
    .agg(F.min("drawdown").alias("drawdown_max"))
)

gold = metriques.join(dd, on="ticker", how="left")
display(gold.orderBy(F.desc("sharpe")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(gold.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable("gold_metriques"))

display(spark.sql("SELECT * FROM gold_metriques ORDER BY sharpe DESC"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fx = (spark.read.parquet("Files/bronze_fx")
      .withColumn("date", F.to_date("date")))

silver_fx = silver.join(fx, on=["date", "devise"], how="left")

w_fx = (Window.partitionBy("devise").orderBy("date")
        .rowsBetween(Window.unboundedPreceding, 0))

silver_fx = (silver_fx
    .withColumn("taux_usd", F.last("taux_usd", ignorenulls=True).over(w_fx))
    .withColumn("close_usd", F.col("close_ajuste") * F.col("taux_usd"))
)

display(silver_fx.filter(F.col("taux_usd").isNull()).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

w = Window.partitionBy("ticker").orderBy("date")

silver_fx = (silver_fx
    .withColumn("close_usd_veille", F.lag("close_usd").over(w))
    .withColumn("rendement_usd",
        (F.col("close_usd") - F.col("close_usd_veille")) / F.col("close_usd_veille"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
