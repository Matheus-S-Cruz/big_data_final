"""Gold — machine learning (R2, seção 5.3).

Lê a Silver e grava três tabelas:
  - gold/propensao_sessao:      probabilidade de compra por sessão (classificação).
  - gold/metricas_modelo:       AUC, F1, precisão, recall no teste + lista de features.
  - gold/segmentos_visitantes:  grupo de cada visitante (K-Means) e perfil médio por grupo.

Restrição anti-vazamento (seção 5.3): as features só usam o comportamento na landing
ANTES do primeiro add_to_cart da sessão. Qualquer evento do checkout (view_cart em diante)
é proibido. O rótulo é verdadeiro quando a sessão contém um purchase.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
    ClusteringEvaluator,
)

from comum import (
    criar_sessao,
    caminho,
    exigir_camada,
    glob_eventos,
    ler_eventos,
    titulo,
)

# Eventos de checkout em diante: proibidos como fonte de feature (vazamento).
EVENTOS_CHECKOUT = [
    "view_cart",
    "begin_checkout",
    "add_shipping_info",
    "add_payment_info",
    "purchase",
    "refund",
]


def main():
    titulo("GOLD — MACHINE LEARNING")
    spark = criar_sessao("gold_ml")

    try:
        exigir_camada(
            spark,
            glob_eventos("silver"),
            "docker compose run --rm app python pipeline/silver.py",
        )

        eventos = ler_eventos(spark, "silver").filter(
            F.col("session_id").isNotNull()
        )

        # Instante do primeiro add_to_cart de cada sessão (corte anti-vazamento).
        corte = (
            eventos.filter(F.col("event_name") == "add_to_cart")
            .groupBy("session_id")
            .agg(F.min("event_ts").alias("ts_corte"))
        )

        base = eventos.join(corte, on="session_id", how="left")

        # Eventos que podem virar feature: antes do primeiro add_to_cart (ou sem
        # add_to_cart na sessão) E que não são de checkout.
        antes = base.filter(
            (F.col("ts_corte").isNull() | (F.col("event_ts") < F.col("ts_corte")))
            & ~F.col("event_name").isin(EVENTOS_CHECKOUT)
            & (F.col("event_name") != "add_to_cart")
        )

        # Canal e device do primeiro evento da sessão.
        janela_ordem = Window.partitionBy("session_id").orderBy(
            F.col("event_ts").asc(), F.col("event_id").asc()
        )
        primeiro = (
            eventos.withColumn("_ordem", F.row_number().over(janela_ordem))
            .filter(F.col("_ordem") == 1)
            .select(
                "session_id",
                "user_pseudo_id",
                F.concat_ws(
                    " / ",
                    F.coalesce(F.col("traffic.source"), F.lit("(nenhum)")),
                    F.coalesce(F.col("traffic.medium"), F.lit("(nenhum)")),
                ).alias("canal"),
                F.coalesce(F.col("device.category"), F.lit("(nenhum)")).alias(
                    "dispositivo"
                ),
            )
        )

        # Features numéricas a partir dos eventos "antes".
        features = (
            antes.groupBy("session_id")
            .agg(
                F.coalesce(F.max("percent_scrolled"), F.lit(0)).alias("scroll_maximo"),
                F.coalesce(F.max("video_percent"), F.lit(0.0)).alias("video_maximo"),
                F.sum(
                    F.when(F.col("event_name") == "view_item_list", 1).otherwise(0)
                ).alias("viu_planos"),
                F.sum(
                    F.when(F.col("event_name") == "select_item", 1).otherwise(0)
                ).alias("selecionou_plano"),
                F.sum(
                    F.when(F.col("event_name") == "view_item", 1).otherwise(0)
                ).alias("abriu_detalhe"),
                F.sum(
                    F.when(F.col("event_name") == "site_search", 1).otherwise(0)
                ).alias("buscas"),
                F.sum(
                    F.when(F.col("event_name") == "file_download", 1).otherwise(0)
                ).alias("downloads"),
                F.sum(
                    F.when(F.col("event_name") == "click", 1).otherwise(0)
                ).alias("cliques"),
                F.count("*").alias("eventos_antes"),
            )
        )

        # Rótulo: a sessão tem purchase?
        rotulo = (
            eventos.groupBy("session_id").agg(
                F.max(
                    F.when(F.col("event_name") == "purchase", 1).otherwise(0)
                ).alias("comprou")
            )
        )

        dataset = (
            primeiro
            .join(features, on="session_id", how="left")
            .join(rotulo, on="session_id", how="left")
            .na.fill(
                {
                    "scroll_maximo": 0,
                    "video_maximo": 0.0,
                    "viu_planos": 0,
                    "selecionou_plano": 0,
                    "abriu_detalhe": 0,
                    "buscas": 0,
                    "downloads": 0,
                    "cliques": 0,
                    "eventos_antes": 0,
                    "comprou": 0,
                }
            )
        )

        cols_numericas = [
            "scroll_maximo",
            "video_maximo",
            "viu_planos",
            "selecionou_plano",
            "abriu_detalhe",
            "buscas",
            "downloads",
            "cliques",
            "eventos_antes",
        ]
        cols_categoricas = ["canal", "dispositivo"]
        features_usadas = cols_numericas + cols_categoricas

        # Pipeline de classificação.
        indexers = [
            StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
            for c in cols_categoricas
        ]
        encoders = [
            OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_ohe")
            for c in cols_categoricas
        ]
        assembler = VectorAssembler(
            inputCols=cols_numericas + [f"{c}_ohe" for c in cols_categoricas],
            outputCol="features",
        )
        lr = LogisticRegression(
            featuresCol="features", labelCol="comprou", maxIter=50
        )
        pipeline = Pipeline(stages=indexers + encoders + [assembler, lr])

        treino, teste = dataset.randomSplit([0.8, 0.2], seed=42)
        modelo = pipeline.fit(treino)

        pred_teste = modelo.transform(teste)
        pred_todas = modelo.transform(dataset)

        auc = BinaryClassificationEvaluator(
            labelCol="comprou", rawPredictionCol="rawPrediction",
            metricName="areaUnderROC",
        ).evaluate(pred_teste)
        f1 = MulticlassClassificationEvaluator(
            labelCol="comprou", predictionCol="prediction", metricName="f1"
        ).evaluate(pred_teste)
        precisao = MulticlassClassificationEvaluator(
            labelCol="comprou", predictionCol="prediction",
            metricName="precisionByLabel", metricLabel=1.0,
        ).evaluate(pred_teste)
        recall = MulticlassClassificationEvaluator(
            labelCol="comprou", predictionCol="prediction",
            metricName="recallByLabel", metricLabel=1.0,
        ).evaluate(pred_teste)

        # ---- propensao_sessao: probabilidade da classe positiva (compra) ----
        extrair_prob = F.udf(lambda v: float(v[1]), "double")
        propensao_sessao = pred_todas.select(
            "session_id",
            "user_pseudo_id",
            "comprou",
            extrair_prob("probability").alias("probabilidade_compra"),
            F.col("prediction").alias("previsao"),
        )
        propensao_sessao.write.mode("overwrite").parquet(
            caminho("gold/propensao_sessao")
        )

        # ---- metricas_modelo ----
        metricas_modelo = spark.createDataFrame(
            [(float(auc), float(f1), float(precisao), float(recall),
              ", ".join(features_usadas))],
            "auc double, f1 double, precisao double, recall double, features string",
        )
        metricas_modelo.write.mode("overwrite").parquet(
            caminho("gold/metricas_modelo")
        )

        # ---- segmentos_visitantes: K-Means por visitante (user_pseudo_id) ----
        # Agrega features por visitante (média das sessões).
        por_visitante = (
            dataset.groupBy("user_pseudo_id")
            .agg(*[F.avg(c).alias(c) for c in cols_numericas])
        )
        assembler_cluster = VectorAssembler(
            inputCols=cols_numericas, outputCol="_vec"
        )
        scaler = StandardScaler(
            inputCol="_vec", outputCol="features", withMean=True, withStd=True
        )
        km = KMeans(featuresCol="features", k=4, seed=42)
        pipe_cluster = Pipeline(stages=[assembler_cluster, scaler, km])
        modelo_cluster = pipe_cluster.fit(por_visitante)
        atribuidos = modelo_cluster.transform(por_visitante)

        silhueta = ClusteringEvaluator(featuresCol="features").evaluate(atribuidos)
        print(f"Silhueta do K-Means (k=4): {silhueta:.4f}")

        # Perfil médio de cada grupo + o grupo de cada visitante.
        perfil = (
            atribuidos.groupBy("prediction")
            .agg(
                F.count("*").alias("visitantes"),
                *[F.round(F.avg(c), 4).alias(f"media_{c}") for c in cols_numericas],
            )
            .withColumnRenamed("prediction", "grupo")
            .orderBy("grupo")
        )
        segmentos_visitantes = atribuidos.select(
            "user_pseudo_id",
            F.col("prediction").alias("grupo"),
            *cols_numericas,
        )
        segmentos_visitantes.write.mode("overwrite").parquet(
            caminho("gold/segmentos_visitantes")
        )
        perfil.write.mode("overwrite").parquet(
            caminho("gold/segmentos_visitantes_perfil")
        )

        print("metricas_modelo:")
        metricas_modelo.show(truncate=False)
        print("propensao_sessao (amostra):")
        propensao_sessao.orderBy(F.desc("probabilidade_compra")).show(10, False)
        print("perfil dos grupos:")
        perfil.show(10, False)

        titulo("GOLD ML CONCLUÍDA")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
