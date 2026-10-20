"""Gold — mapa de calor (R2, seção 5.3, R11).

Lê a Silver e grava duas tabelas com NOMES DE COLUNA EXATOS (o site depende deles):
  - gold/mapa_calor_cliques: pagina, element_id, cliques, sessoes, sessoes_na_pagina,
                             taxa_clique, intensidade.
  - gold/mapa_calor_rolagem: pagina, marco, sessoes_que_chegaram, pct_sessoes.

Definições (seção 5.4):
  - pagina = caminho de page_location, sem domínio, sem '#' e sem '?'. Raiz vira '/'.
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

MARCOS_ROLAGEM = [0, 25, 50, 75, 90]


def coluna_pagina(col_location):
    """page_location -> caminho sem domínio, sem '#' e sem '?'. Raiz vira '/'."""
    sem_fragmento = F.split(col_location, "#").getItem(0)
    sem_query = F.split(sem_fragmento, r"\?").getItem(0)
    # Remove esquema://host, mantendo o caminho a partir da primeira '/'.
    caminho_rel = F.regexp_replace(sem_query, r"^[a-zA-Z]+://[^/]+", "")
    caminho_rel = F.regexp_replace(caminho_rel, r"/+$", "")  # tira barra final
    return F.when(
        (caminho_rel == "") | caminho_rel.isNull(), F.lit("/")
    ).otherwise(caminho_rel)


def main():
    titulo("GOLD — MAPA DE CALOR")
    spark = criar_sessao("gold_mapa_calor")

    try:
        exigir_camada(
            spark,
            glob_eventos("silver"),
            "docker compose run --rm app python pipeline/silver.py",
        )

        eventos = ler_eventos(spark, "silver").filter(
            F.col("session_id").isNotNull()
        )

        com_pagina = eventos.withColumn("pagina", coluna_pagina(F.col("page.location")))

        # sessoes_na_pagina: sessões com page_view em cada página.
        sessoes_na_pagina = (
            com_pagina.filter(F.col("event_name") == "page_view")
            .groupBy("pagina")
            .agg(F.countDistinct("session_id").alias("sessoes_na_pagina"))
        )

        # ---- mapa_calor_cliques ----
        base_cliques = com_pagina.filter(
            F.col("element_id").isNotNull()
            & F.col("page.location").isNotNull()
        )

        agregado = (
            base_cliques.groupBy("pagina", "element_id")
            .agg(
                F.count("*").alias("cliques"),
                F.countDistinct("session_id").alias("sessoes"),
            )
            .join(sessoes_na_pagina, on="pagina", how="left")
        )

        janela_pagina = Window.partitionBy("pagina")
        mapa_calor_cliques = (
            agregado
            .withColumn(
                "taxa_clique",
                F.round(
                    F.try_divide(F.col("sessoes"), F.col("sessoes_na_pagina")), 4
                ),
            )
            .withColumn("_max_cliques", F.max("cliques").over(janela_pagina))
            .withColumn(
                "intensidade",
                F.round(F.try_divide(F.col("cliques"), F.col("_max_cliques")), 4),
            )
            .select(
                "pagina",
                "element_id",
                "cliques",
                "sessoes",
                "sessoes_na_pagina",
                "taxa_clique",
                "intensidade",
            )
        )
        mapa_calor_cliques.write.mode("overwrite").parquet(
            caminho("gold/mapa_calor_cliques")
        )

        # ---- mapa_calor_rolagem (só a landing, pagina = "/") ----
        # Sessões com page_view na landing.
        sessoes_landing = (
            com_pagina.filter(
                (F.col("event_name") == "page_view") & (F.col("pagina") == "/")
            )
            .select("session_id")
            .distinct()
        )
        total_landing = sessoes_landing.count()

        # Maior percent_scrolled por sessão na landing.
        scroll_landing = (
            com_pagina.filter(
                (F.col("event_name") == "scroll") & (F.col("pagina") == "/")
            )
            .groupBy("session_id")
            .agg(F.max("percent_scrolled").alias("scroll_max"))
        )
        base_rolagem = sessoes_landing.join(
            scroll_landing, on="session_id", how="left"
        )

        linhas = []
        for m in MARCOS_ROLAGEM:
            if m == 0:
                chegaram = total_landing
            else:
                chegaram = base_rolagem.filter(
                    F.col("scroll_max") >= m
                ).count()
            pct = round(chegaram / total_landing, 4) if total_landing else None
            linhas.append(("/", m, chegaram, pct))

        mapa_calor_rolagem = spark.createDataFrame(
            linhas,
            "pagina string, marco int, sessoes_que_chegaram long, pct_sessoes double",
        ).orderBy("marco")
        mapa_calor_rolagem.write.mode("overwrite").parquet(
            caminho("gold/mapa_calor_rolagem")
        )

        print("mapa_calor_cliques (amostra):")
        mapa_calor_cliques.orderBy(F.desc("cliques")).show(20, False)
        print("mapa_calor_rolagem:")
        mapa_calor_rolagem.show(10, False)

        titulo("GOLD MAPA DE CALOR CONCLUÍDA")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
