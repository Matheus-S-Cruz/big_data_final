"""Gold — funil de compra e funil de lead (R2, seção 5.3).

Lê a Silver e grava duas tabelas:
  - gold/funil_compra: sessões em cada etapa do funil, conversões, total e por plano x canal.
  - gold/funil_lead:   page_view -> scroll >= 50% -> generate_lead, por canal.

Definições (seção 5.4):
  - Canal da sessão: "<source> / <medium>" do PRIMEIRO evento da sessão.
  - Plano da sessão:  item_id do primeiro select_item da sessão, "(nenhum)" se não houver.
  - Funil: uma sessão conta numa etapa se teve aquele evento em qualquer momento
           (não exige as etapas anteriores).
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

# Etapas do funil de compra, na ordem. view_item NÃO entra aqui (é taxa à parte).
ETAPAS_FUNIL = [
    "view_item_list",
    "select_item",
    "add_to_cart",
    "view_cart",
    "begin_checkout",
    "add_payment_info",
    "purchase",
]


def canal_e_plano_por_sessao(eventos):
    """Devolve um DataFrame com session_id, canal e plano de cada sessão."""
    # Canal = source / medium do primeiro evento da sessão (menor event_ts).
    janela_ordem = Window.partitionBy("session_id").orderBy(
        F.col("event_ts").asc(), F.col("event_id").asc()
    )

    canal = (
        eventos
        .withColumn("_ordem", F.row_number().over(janela_ordem))
        .filter(F.col("_ordem") == 1)
        .withColumn(
            "canal",
            F.concat_ws(
                " / ",
                F.coalesce(F.col("traffic.source"), F.lit("(nenhum)")),
                F.coalesce(F.col("traffic.medium"), F.lit("(nenhum)")),
            ),
        )
        .select("session_id", "canal")
    )

    # Plano = item_id do primeiro select_item da sessão.
    janela_select = Window.partitionBy("session_id").orderBy(
        F.col("event_ts").asc(), F.col("event_id").asc()
    )
    selects = (
        eventos
        .filter(F.col("event_name") == "select_item")
    )
    # item_id do select_item vem da tabela de itens; aqui usamos a coluna item_list
    # não serve. O select_item carrega o plano no items[], então pegamos do primeiro item.
    selects = (
        selects
        .withColumn("_item", F.element_at("items", 1))
        .withColumn("_item_id", F.col("_item.item_id"))
        .withColumn("_ordem", F.row_number().over(janela_select))
        .filter(F.col("_ordem") == 1)
        .select("session_id", F.col("_item_id").alias("plano"))
    )

    return canal.join(selects, on="session_id", how="left").withColumn(
        "plano", F.coalesce(F.col("plano"), F.lit("(nenhum)"))
    )


def main():
    titulo("GOLD — FUNIL")
    spark = criar_sessao("gold_funil")

    try:
        exigir_camada(
            spark,
            glob_eventos("silver"),
            "docker compose run --rm app python pipeline/silver.py",
        )

        eventos = ler_eventos(spark, "silver").filter(
            F.col("session_id").isNotNull()
        )

        dim = canal_e_plano_por_sessao(eventos)

        # Flags: a sessão atingiu cada etapa (teve o evento em qualquer momento)?
        etapas_df = (
            eventos
            .filter(F.col("event_name").isin(ETAPAS_FUNIL + ["view_item"]))
            .groupBy("session_id")
            .pivot("event_name", ETAPAS_FUNIL + ["view_item"])
            .agg(F.lit(1))
        )

        sessoes = (
            dim.join(etapas_df, on="session_id", how="left")
            .na.fill(0, ETAPAS_FUNIL + ["view_item"])
        )

        # ---- funil_compra: total e por plano x canal ----
        def montar_funil(df, cols_grupo):
            agg = df.groupBy(*cols_grupo).agg(
                *[F.sum(e).alias(e) for e in ETAPAS_FUNIL],
                F.sum("view_item").alias("view_item"),
                F.sum("select_item").alias("_select_para_taxa"),
            )
            # Converte em formato longo: uma linha por etapa.
            etapas_expr = F.array(
                *[
                    F.struct(
                        F.lit(i).alias("ordem_etapa"),
                        F.lit(nome).alias("etapa"),
                        F.col(nome).alias("sessoes"),
                    )
                    for i, nome in enumerate(ETAPAS_FUNIL)
                ]
            )
            longo = (
                agg
                .withColumn("_e", F.explode(etapas_expr))
                .select(
                    *cols_grupo,
                    F.col("_e.ordem_etapa").alias("ordem_etapa"),
                    F.col("_e.etapa").alias("etapa"),
                    F.col("_e.sessoes").alias("sessoes"),
                    F.col("view_item"),
                    F.col("_select_para_taxa"),
                )
            )
            # Conversões: sobre o topo (view_item_list) e sobre a etapa anterior.
            w = Window.partitionBy(*cols_grupo).orderBy("ordem_etapa")
            topo = Window.partitionBy(*cols_grupo)
            longo = (
                longo
                .withColumn("_sessoes_topo", F.first("sessoes").over(w))
                .withColumn("_sessoes_anterior", F.lag("sessoes").over(w))
                .withColumn(
                    "conversao_sobre_topo",
                    F.round(F.try_divide(F.col("sessoes"), F.col("_sessoes_topo")), 4),
                )
                .withColumn(
                    "conversao_etapa_anterior",
                    F.round(
                        F.try_divide(F.col("sessoes"), F.col("_sessoes_anterior")), 4
                    ),
                )
                # Taxa de view_item (à parte): sessoes com view_item / sessoes com select_item.
                .withColumn(
                    "taxa_view_item",
                    F.round(
                        F.try_divide(
                            F.first("view_item").over(topo),
                            F.first("_select_para_taxa").over(topo),
                        ),
                        4,
                    ),
                )
                .drop("_sessoes_topo", "_sessoes_anterior", "_select_para_taxa")
            )
            return longo

        funil_total = montar_funil(sessoes, []).withColumn(
            "plano", F.lit("(total)")
        ).withColumn("canal", F.lit("(total)"))

        funil_segmentado = montar_funil(sessoes, ["plano", "canal"])

        funil_compra = funil_total.unionByName(funil_segmentado)

        funil_compra.write.mode("overwrite").parquet(caminho("gold/funil_compra"))

        # ---- funil_lead: page_view -> scroll >= 50% -> generate_lead, por canal ----
        flags_lead = (
            eventos
            .groupBy("session_id")
            .agg(
                F.max(
                    F.when(
                        (F.col("event_name") == "page_view")
                        & (F.col("page.location").contains("loja.exemplo")),
                        1,
                    ).otherwise(0)
                ).alias("tem_page_view"),
                F.max(
                    F.when(
                        (F.col("event_name") == "scroll")
                        & (F.col("percent_scrolled") >= 50),
                        1,
                    ).otherwise(0)
                ).alias("tem_scroll_50"),
                F.max(
                    F.when(F.col("event_name") == "generate_lead", 1).otherwise(0)
                ).alias("tem_lead"),
            )
        )
        funil_lead = (
            flags_lead.join(dim.select("session_id", "canal"), on="session_id")
            .groupBy("canal")
            .agg(
                F.sum("tem_page_view").alias("sessoes_page_view"),
                F.sum("tem_scroll_50").alias("sessoes_scroll_50"),
                F.sum("tem_lead").alias("sessoes_lead"),
            )
            .withColumn(
                "conv_scroll_sobre_pageview",
                F.round(
                    F.try_divide(
                        F.col("sessoes_scroll_50"), F.col("sessoes_page_view")
                    ),
                    4,
                ),
            )
            .withColumn(
                "conv_lead_sobre_scroll",
                F.round(
                    F.try_divide(F.col("sessoes_lead"), F.col("sessoes_scroll_50")),
                    4,
                ),
            )
        )

        funil_lead.write.mode("overwrite").parquet(caminho("gold/funil_lead"))

        print("funil_compra (amostra):")
        funil_compra.filter(F.col("plano") == "(total)").orderBy(
            "ordem_etapa"
        ).show(20, False)
        print("funil_lead:")
        funil_lead.orderBy(F.desc("sessoes_page_view")).show(20, False)

        titulo("GOLD FUNIL CONCLUÍDA")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
