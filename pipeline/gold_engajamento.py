"""Gold — engajamento (R2, seção 5.3).

Lê a Silver e grava quatro tabelas:
  - gold/sessoes:          uma linha por sessão (duração, eventos, scroll, vídeo, lead, compra, canal, device).
  - gold/retencao_video:   das sessões com video_start, % que atingiu 10/25/50/75/100.
  - gold/buscas_faq:       termo, total de buscas, buscas sem resultado.
  - gold/saidas_downloads: cliques de saída por domínio; downloads por arquivo.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from comum import (
    criar_sessao,
    caminho,
    exigir_camada,
    glob_eventos,
    ler_eventos,
    titulo,
)


def main():
    titulo("GOLD — ENGAJAMENTO")
    spark = criar_sessao("gold_engajamento")

    try:
        exigir_camada(
            spark,
            glob_eventos("silver"),
            "docker compose run --rm app python pipeline/silver.py",
        )

        eventos = ler_eventos(spark, "silver").filter(
            F.col("session_id").isNotNull()
        )

        # Canal e device do primeiro evento da sessão.
        janela_ordem = Window.partitionBy("session_id").orderBy(
            F.col("event_ts").asc(), F.col("event_id").asc()
        )
        primeiro = (
            eventos
            .withColumn("_ordem", F.row_number().over(janela_ordem))
            .filter(F.col("_ordem") == 1)
            .select(
                "session_id",
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

        # ---- sessoes ----
        sessoes = (
            eventos
            .groupBy("session_id", "user_pseudo_id")
            .agg(
                F.min("event_ts").alias("inicio"),
                F.max("event_ts").alias("fim"),
                F.count("*").alias("eventos"),
                F.max("percent_scrolled").alias("scroll_maximo"),
                F.max("video_percent").alias("video_maximo"),
                F.max(
                    F.when(F.col("event_name") == "generate_lead", 1).otherwise(0)
                ).alias("fez_lead"),
                F.max(
                    F.when(F.col("event_name") == "purchase", 1).otherwise(0)
                ).alias("comprou"),
            )
            .withColumn(
                "duracao_segundos",
                F.col("fim").cast("long") - F.col("inicio").cast("long"),
            )
            .join(primeiro, on="session_id", how="left")
            .select(
                "session_id",
                "user_pseudo_id",
                "inicio",
                "fim",
                "duracao_segundos",
                "eventos",
                "scroll_maximo",
                "video_maximo",
                "fez_lead",
                "comprou",
                "canal",
                "dispositivo",
            )
        )
        sessoes.write.mode("overwrite").parquet(caminho("gold/sessoes"))

        # ---- retencao_video ----
        # Base: sessões que tiveram video_start.
        sessoes_video = (
            eventos
            .filter(F.col("event_name") == "video_start")
            .select("session_id")
            .distinct()
        )
        # Maior video_percent atingido por sessão (dentre eventos de vídeo).
        maior_percent = (
            eventos
            .filter(F.col("event_name").startswith("video_"))
            .groupBy("session_id")
            .agg(F.max("video_percent").alias("maior_video_percent"))
        )
        base_video = sessoes_video.join(maior_percent, on="session_id", how="left")
        total_video = base_video.count()

        marcos_video = [10, 25, 50, 75, 100]
        linhas_video = base_video.agg(
            F.lit(total_video).alias("sessoes_com_video_start"),
            *[
                F.sum(
                    F.when(F.col("maior_video_percent") >= m, 1).otherwise(0)
                ).alias(f"atingiu_{m}")
                for m in marcos_video
            ],
        )
        # Formato longo: uma linha por marco.
        retencao_video = linhas_video.select(
            F.explode(
                F.array(
                    *[
                        F.struct(
                            F.lit(m).alias("marco"),
                            F.col(f"atingiu_{m}").alias("sessoes"),
                            F.col("sessoes_com_video_start").alias(
                                "sessoes_com_video_start"
                            ),
                        )
                        for m in marcos_video
                    ]
                )
            ).alias("_r")
        ).select(
            F.col("_r.marco").alias("marco"),
            F.col("_r.sessoes").alias("sessoes_que_atingiram"),
            F.col("_r.sessoes_com_video_start").alias("sessoes_com_video_start"),
            F.round(
                F.try_divide(
                    F.col("_r.sessoes"), F.col("_r.sessoes_com_video_start")
                ),
                4,
            ).alias("pct_retencao"),
        )
        retencao_video.write.mode("overwrite").parquet(
            caminho("gold/retencao_video")
        )

        # ---- buscas_faq ----
        buscas_faq = (
            eventos
            .filter(F.col("event_name") == "site_search")
            .groupBy(F.col("search_term").alias("termo"))
            .agg(
                F.count("*").alias("total_buscas"),
                F.sum(
                    F.when(
                        F.coalesce(F.col("search_results"), F.lit(0)) == 0, 1
                    ).otherwise(0)
                ).alias("buscas_sem_resultado"),
            )
            .orderBy(F.desc("total_buscas"))
        )
        buscas_faq.write.mode("overwrite").parquet(caminho("gold/buscas_faq"))

        # ---- saidas_downloads ----
        saidas = (
            eventos
            .filter(
                (F.col("event_name") == "click") & (F.col("outbound") == True)  # noqa: E712
            )
            .groupBy(F.col("link_domain").alias("chave"))
            .agg(F.count("*").alias("quantidade"))
            .withColumn("tipo", F.lit("saida"))
        )
        downloads = (
            eventos
            .filter(F.col("event_name") == "file_download")
            .groupBy(F.col("file_name").alias("chave"))
            .agg(F.count("*").alias("quantidade"))
            .withColumn("tipo", F.lit("download"))
        )
        saidas_downloads = saidas.unionByName(downloads).select(
            "tipo", "chave", "quantidade"
        ).orderBy("tipo", F.desc("quantidade"))
        saidas_downloads.write.mode("overwrite").parquet(
            caminho("gold/saidas_downloads")
        )

        print("sessoes (amostra):")
        sessoes.show(5, False)
        print("retencao_video:")
        retencao_video.orderBy("marco").show(10, False)
        print("buscas_faq (top):")
        buscas_faq.show(10, False)
        print("saidas_downloads:")
        saidas_downloads.show(20, False)

        titulo("GOLD ENGAJAMENTO CONCLUÍDA")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
