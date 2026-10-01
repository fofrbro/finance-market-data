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

from pyspark.sql import functions as F


# Table dim_date
dates = (spark.sql("SELECT explode(sequence(to_date('2015-01-01'), to_date('2026-12-31'), interval 1 day)) AS date")
    .withColumn("annee", F.year("date"))
    .withColumn("mois", F.month("date"))
    .withColumn("trimestre", F.quarter("date"))
    .withColumn("nom_mois", F.date_format("date", "MMMM"))
    .withColumn("annee_mois", F.date_format("date", "yyyy-MM")))

dates.write.format("delta").mode("overwrite").saveAsTable("dim_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Table dim_instrument
(spark.table("silver_cours")
    .select("ticker", "nom", "place", "devise", "type_instrument")
    .distinct()
    .write.format("delta").mode("overwrite").saveAsTable("dim_instrument"))

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
