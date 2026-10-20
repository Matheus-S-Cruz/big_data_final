"""Gold — receita e aquisição (R2, seção 5.3).

Lê a Silver e grava quatro tabelas:
  - gold/receita_diaria:    por data da compra (UTC), plano e canal: bruta, reembolsada, líquida, pedidos, ticket médio.
  - gold/taxa_reembolso:    por plano e canal.
  - gold/adesao_order_bump: % de compras com BUMP-MENTORIA.
  - gold/lead_para_compra:  leads, quantos compraram depois, mediana de dias (total e por canal do lead).

Definições (seção 5.4):
  - Receita bruta: soma de value dos purchase válidos.
  - Reembolsada:   soma de value dos refund válidos, ligada à compra pelo transaction_id.
  - Líquida:       bruta - reembolsada.    Ticket médio: bruta / pedidos.
  - Plano da compra: item de categoria "plano" no items[] do purchase.
  - Canal da compra: canal da sessão em que ocorreu (source/medium do 1º evento).
  - Lead -> compra: unidade = pessoa (user_id resolvido, senão user_pseudo_id);
                    1º generate_lead e 1ª compra estritamente depois; dias = seg/86400; mediana.
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
    titulo("GOLD — RECEITA E AQUISIÇÃO")
    spark = criar_sessao("gold_receita")

    try:
        exigir_camada(
            spark,
            glob_eventos("silver"),
            "docker compose run --rm app python pipeline/silver.py",
        )

        eventos = ler_eventos(spark, "silver")
        itens = spark.read.parquet(caminho("silver/itens"))

        # Canal de cada sessão (source/medium do primeiro evento).
        janela_ordem = Window.partitionBy("session_id").orderBy(
            F.col("event_ts").asc(), F.col("event_id").asc()
        )
        canal_sessao = (
            eventos.filter(F.col("session_id").isNotNull())
            .withColumn("_ordem", F.row_number().over(janela_ordem))
            .filter(F.col("_ordem") == 1)
            .select(
                "session_id",
                F.concat_ws(
                    " / ",
                    F.coalesce(F.col("traffic.source"), F.lit("(nenhum)")),
                    F.coalesce(F.col("traffic.medium"), F.lit("(nenhum)")),
                ).alias("canal"),
            )
        )

        # Plano de cada compra = item de categoria "plano" no items do purchase.
        plano_compra = (
            itens.filter(
                (F.col("event_name") == "purchase")
                & (F.col("item_category") == "plano")
            )
            .groupBy("event_id")
            .agg(F.first("item_id").alias("plano"))
        )

        # Compras válidas (já estão validadas na Silver).
        compras = (
            eventos.filter(F.col("event_name") == "purchase")
            .join(canal_sessao, on="session_id", how="left")
            .join(plano_compra, on="event_id", how="left")
            .select(
                "event_id",
                "transaction_id",
                "session_id",
                F.col("value").alias("valor_bruto"),
                F.to_date("event_ts").alias("data_compra"),
                F.coalesce(F.col("canal"), F.lit("(nenhum)")).alias("canal"),
                F.coalesce(F.col("plano"), F.lit("(nenhum)")).alias("plano"),
            )
        )

        # Reembolsos válidos, ligados à compra pelo transaction_id.
        reembolsos = (
            eventos.filter(F.col("event_name") == "refund")
            .groupBy("transaction_id")
            .agg(F.sum("value").alias("valor_reembolsado"))
        )

        compras_com_reembolso = compras.join(
            reembolsos, on="transaction_id", how="left"
        ).withColumn(
            "valor_reembolsado",
            F.coalesce(F.col("valor_reembolsado"), F.lit(0.0)),
        )

        # ---- receita_diaria ----
        receita_diaria = (
            compras_com_reembolso.groupBy("data_compra", "plano", "canal")
            .agg(
                F.sum("valor_bruto").alias("receita_bruta"),
                F.sum("valor_reembolsado").alias("receita_reembolsada"),
                F.count("*").alias("pedidos"),
            )
            .withColumn(
                "receita_liquida",
                F.col("receita_bruta") - F.col("receita_reembolsada"),
            )
            .withColumn(
                "ticket_medio",
                F.round(F.try_divide(F.col("receita_bruta"), F.col("pedidos")), 2),
            )
            .orderBy("data_compra", "plano", "canal")
        )
        receita_diaria.write.mode("overwrite").parquet(
            caminho("gold/receita_diaria")
        )

        # ---- taxa_reembolso: pedidos com reembolso / pedidos (por plano e canal) ----
        taxa_reembolso = (
            compras_com_reembolso.groupBy("plano", "canal")
            .agg(
                F.count("*").alias("pedidos"),
                F.sum(
                    F.when(F.col("valor_reembolsado") > 0, 1).otherwise(0)
                ).alias("pedidos_com_reembolso"),
            )
            .withColumn(
                "taxa_reembolso",
                F.round(
                    F.try_divide(
                        F.col("pedidos_com_reembolso"), F.col("pedidos")
                    ),
                    4,
                ),
            )
            .orderBy("plano", "canal")
        )
        taxa_reembolso.write.mode("overwrite").parquet(
            caminho("gold/taxa_reembolso")
        )

        # ---- adesao_order_bump: % de compras com BUMP-MENTORIA ----
        compras_com_bump = (
            itens.filter(
                (F.col("event_name") == "purchase")
                & (F.col("item_id") == "BUMP-MENTORIA")
            )
            .select("event_id")
            .distinct()
            .withColumn("_tem_bump", F.lit(1))
        )
        total_pedidos = compras.count()
        pedidos_bump = compras_com_bump.count()
        adesao_order_bump = spark.createDataFrame(
            [(total_pedidos, pedidos_bump)],
            "pedidos long, pedidos_com_bump long",
        ).withColumn(
            "adesao",
            F.round(F.try_divide(F.col("pedidos_com_bump"), F.col("pedidos")), 4),
        )
        adesao_order_bump.write.mode("overwrite").parquet(
            caminho("gold/adesao_order_bump")
        )

        # ---- lead_para_compra ----
        # Unidade = pessoa: user_id resolvido, senão user_pseudo_id.
        pessoa = F.coalesce(F.col("user_id"), F.col("user_pseudo_id"))

        primeiro_lead = (
            eventos.filter(F.col("event_name") == "generate_lead")
            .withColumn("pessoa", pessoa)
            .groupBy("pessoa")
            .agg(F.min("event_ts").alias("ts_lead"))
        )
        # Canal do lead = canal da sessão do primeiro generate_lead.
        janela_lead = Window.partitionBy(pessoa).orderBy(F.col("event_ts").asc())
        canal_do_lead = (
            eventos.filter(F.col("event_name") == "generate_lead")
            .withColumn("pessoa", pessoa)
            .withColumn("_ordem", F.row_number().over(janela_lead))
            .filter(F.col("_ordem") == 1)
            .join(canal_sessao, on="session_id", how="left")
            .select(
                "pessoa",
                F.coalesce(F.col("canal"), F.lit("(nenhum)")).alias("canal_lead"),
            )
        )

        compras_pessoa = (
            eventos.filter(F.col("event_name") == "purchase")
            .withColumn("pessoa", pessoa)
            .groupBy("pessoa")
            .agg(F.min("event_ts").alias("ts_compra"))
        )

        lead_join = (
            primeiro_lead.join(canal_do_lead, on="pessoa", how="left")
            .join(compras_pessoa, on="pessoa", how="left")
            .withColumn(
                "comprou_depois",
                F.when(
                    F.col("ts_compra").isNotNull()
                    & (F.col("ts_compra") > F.col("ts_lead")),
                    1,
                ).otherwise(0),
            )
            .withColumn(
                "dias",
                F.when(
                    F.col("comprou_depois") == 1,
                    (F.col("ts_compra").cast("double") - F.col("ts_lead").cast("double"))
                    / F.lit(86400.0),
                ),
            )
        )

        def resumo_lead(df, cols_grupo):
            return (
                df.groupBy(*cols_grupo).agg(
                    F.count("*").alias("leads"),
                    F.sum("comprou_depois").alias("leads_que_compraram"),
                    F.round(
                        F.percentile_approx("dias", 0.5), 4
                    ).alias("mediana_dias_lead_compra"),
                )
            )

        total_df = resumo_lead(lead_join, []).withColumn(
            "canal_lead", F.lit("(total)")
        )
        por_canal_df = resumo_lead(lead_join, ["canal_lead"])
        lead_para_compra = total_df.unionByName(por_canal_df).orderBy(
            F.desc("leads")
        )
        lead_para_compra.write.mode("overwrite").parquet(
            caminho("gold/lead_para_compra")
        )

        print("receita_diaria (amostra):")
        receita_diaria.show(10, False)
        print("taxa_reembolso:")
        taxa_reembolso.show(20, False)
        print("adesao_order_bump:")
        adesao_order_bump.show(False)
        print("lead_para_compra:")
        lead_para_compra.show(20, False)

        titulo("GOLD RECEITA CONCLUÍDA")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
