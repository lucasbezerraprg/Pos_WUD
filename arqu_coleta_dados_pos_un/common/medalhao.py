"""
medalhao.py - Bronze -> Silver -> Gold (tabelas Iceberg no catálogo `lake`).

  lake.bronze.*  dado cru + rastreabilidade (_ingest_ts, _source_file, _batch_id)
  lake.silver.*  limpo, tipado, deduplicado; o que falha vai para a quarentena
  lake.gold.*    pronto para consumo (BI e vetorial)

A Silver é reconstruída a partir de TODA a Bronze (todos os lotes). Por isso
reexecutar é seguro (idempotente) e um lote novo entra sozinho.
"""
from pyspark.sql import functions as F
from pyspark.sql import Window

EMAIL_RE = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"


# ----------------------------------------------------------------- Bronze
def to_bronze(spark, df, tabela, batch):
    """Grava um DataFrame cru na Bronze, acrescentando colunas de rastreabilidade.
    Idempotente por lote: reexecutar o mesmo lote não duplica linhas."""
    out = (df.withColumn("_ingest_ts", F.current_timestamp())
             .withColumn("_source_file", F.input_file_name())
             .withColumn("_batch_id", F.lit(batch)))
    if spark.catalog.tableExists(tabela):
        spark.sql(f"DELETE FROM {tabela} WHERE _batch_id = '{batch}'")
        out.writeTo(tabela).append()
    else:
        out.writeTo(tabela).using("iceberg").partitionedBy(F.col("_batch_id")).create()
    return spark.table(tabela).filter(F.col("_batch_id") == batch).count()


def ingest_lote(spark, batch, landing_uri):
    """Ingere os 4 arquivos de um lote da landing para a Bronze."""
    base = landing_uri(f"mercadonordeste/{batch}")
    res = {}
    res["clientes"] = to_bronze(spark, spark.read.option("header", True).csv(f"{base}/clientes.csv"),
                                "lake.bronze.clientes", batch)
    if batch == "lote1":
        res["produtos"] = to_bronze(spark, spark.read.option("header", True).csv(f"{base}/produtos.csv"),
                                    "lake.bronze.produtos", batch)
    res["pedidos"] = to_bronze(spark, spark.read.json(f"{base}/pedidos.jsonl"), "lake.bronze.pedidos", batch)
    res["avaliacoes"] = to_bronze(spark, spark.read.option("header", True).csv(f"{base}/avaliacoes.csv"),
                                  "lake.bronze.avaliacoes", batch)
    return res


# ------------------------------------------------------------------ Silver
def _parse_data(c):
    return F.coalesce(F.to_date(F.col(c), "yyyy-MM-dd"),
                      F.to_date(F.col(c), "dd/MM/yyyy"),
                      F.to_date(F.to_timestamp(F.col(c), "yyyy-MM-dd'T'HH:mm:ss")))


def _latest(df, key):
    w = Window.partitionBy(key).orderBy(F.col("_ingest_ts").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def limpa_clientes(df):
    d = (df.withColumn("cliente_id", F.col("cliente_id").cast("int"))
           .withColumn("estado", F.upper(F.trim("estado")))
           .withColumn("email", F.lower(F.trim("email")))
           .withColumn("email", F.when(F.col("email").rlike(EMAIL_RE), F.col("email")))
           .withColumn("data_cadastro", _parse_data("data_cadastro")))
    d = _latest(d, "cliente_id")
    return d.filter("cliente_id IS NOT NULL"), d.filter("cliente_id IS NULL")


def limpa_produtos(df):
    d = (df.withColumn("produto_id", F.col("produto_id").cast("int"))
           .withColumn("preco", F.col("preco").cast("double"))
           .withColumn("custo", F.col("custo").cast("double")))
    d = _latest(d, "produto_id")
    ok = (F.col("preco") > 0) & (F.col("custo") > 0)
    return d.filter(ok), d.filter(~ok)


def limpa_pedidos(df, clientes_validos):
    """Devolve (pedidos, itens, quarentena). Deduplica, tipa, explode itens e isola órfãos."""
    d = (df.withColumn("pedido_id", F.col("pedido_id").cast("long"))
           .withColumn("cliente_id", F.col("cliente_id").cast("int"))
           .withColumn("data_pedido", F.to_timestamp("data_pedido", "yyyy-MM-dd'T'HH:mm:ss"))
           .withColumn("valor_frete", F.col("valor_frete").cast("double")))
    d = _latest(d, "pedido_id")
    ids = clientes_validos.select("cliente_id").distinct()
    ok = d.join(ids, "cliente_id", "left_semi")
    orfaos = d.join(ids, "cliente_id", "left_anti")
    pedidos = ok.drop("itens")
    itens = (ok.select("pedido_id", "_ingest_ts", F.posexplode("itens").alias("item_seq", "item"))
               .select("pedido_id", "item_seq",
                       F.col("item.produto_id").cast("int").alias("produto_id"),
                       F.col("item.qtd").cast("int").alias("qtd"),
                       F.col("item.preco_unit").cast("double").alias("preco_unit"),
                       "_ingest_ts")
               .withColumn("valor_item", F.round(F.col("qtd") * F.col("preco_unit"), 2)))
    return pedidos, itens, orfaos.drop("itens")


def limpa_avaliacoes(df):
    d = (df.withColumn("avaliacao_id", F.col("avaliacao_id").cast("long"))
           .withColumn("pedido_id", F.col("pedido_id").cast("long"))
           .withColumn("cliente_id", F.col("cliente_id").cast("int"))
           .withColumn("produto_id", F.col("produto_id").cast("int"))
           .withColumn("nota", F.col("nota").cast("int"))
           .withColumn("data_avaliacao", _parse_data("data_avaliacao")))
    d = _latest(d, "avaliacao_id")
    ok = F.col("nota").between(1, 5)
    return d.filter(ok), d.filter(~ok)


def build_silver(spark):
    """Reconstrói a Silver a partir de toda a Bronze. Devolve {tabela: linhas}."""
    t = lambda n: spark.table(f"lake.bronze.{n}")
    cli, cli_q = limpa_clientes(t("clientes"))
    prod, prod_q = limpa_produtos(t("produtos"))
    cli.writeTo("lake.silver.clientes").using("iceberg").createOrReplace()
    prod.writeTo("lake.silver.produtos").using("iceberg").createOrReplace()
    ped, itens, ped_q = limpa_pedidos(t("pedidos"), spark.table("lake.silver.clientes"))
    ped.writeTo("lake.silver.pedidos").using("iceberg").createOrReplace()
    itens.writeTo("lake.silver.itens_pedido").using("iceberg").createOrReplace()
    av, av_q = limpa_avaliacoes(t("avaliacoes"))
    av.writeTo("lake.silver.avaliacoes").using("iceberg").createOrReplace()
    q = (prod_q.select(F.lit("produtos").alias("origem"), F.to_json(F.struct("produto_id", "preco", "custo")).alias("registro"))
         .unionByName(ped_q.select(F.lit("pedidos_cliente_orfao").alias("origem"),
                                   F.to_json(F.struct("pedido_id", "cliente_id")).alias("registro")))
         .unionByName(av_q.select(F.lit("avaliacoes_nota_invalida").alias("origem"),
                                  F.to_json(F.struct("avaliacao_id", "nota")).alias("registro"))))
    q.withColumn("_quarentena_ts", F.current_timestamp()).writeTo("lake.silver.quarentena").using("iceberg").createOrReplace()
    return {n: spark.table(f"lake.silver.{n}").count()
            for n in ("clientes", "produtos", "pedidos", "itens_pedido", "avaliacoes", "quarentena")}


# ------------------------------------------------------------------ Gold
def build_vendas_diarias(spark):
    ped = spark.table("lake.silver.pedidos").filter("status NOT IN ('cancelado')")
    it = spark.table("lake.silver.itens_pedido")
    prod = spark.table("lake.silver.produtos").select("produto_id", "categoria")
    cli = spark.table("lake.silver.clientes").select("cliente_id", "estado", "canal_aquisicao")
    df = (it.join(ped.select("pedido_id", "cliente_id", "data_pedido", "status"), "pedido_id")
            .join(prod, "produto_id").join(cli, "cliente_id")
            .groupBy(F.to_date("data_pedido").alias("data"), "categoria", "estado", "canal_aquisicao")
            .agg(F.countDistinct("pedido_id").alias("qtd_pedidos"),
                 F.sum("qtd").alias("qtd_itens"),
                 F.round(F.sum("valor_item"), 2).alias("receita")))
    df.writeTo("lake.gold.vendas_diarias").using("iceberg").createOrReplace()
    return spark.table("lake.gold.vendas_diarias")


def build_clientes_360(spark):
    ped = spark.table("lake.silver.pedidos").filter("status NOT IN ('cancelado')")
    it = spark.table("lake.silver.itens_pedido")
    cli = spark.table("lake.silver.clientes")
    av = spark.table("lake.silver.avaliacoes")
    ref = ped.agg(F.max(F.to_date("data_pedido"))).first()[0]
    tot = it.groupBy("pedido_id").agg(F.sum("valor_item").alias("valor"))
    f = (ped.join(tot, "pedido_id").groupBy("cliente_id")
            .agg(F.count("*").alias("qtd_pedidos"), F.round(F.sum("valor"), 2).alias("receita_total"),
                 F.round(F.avg("valor"), 2).alias("ticket_medio"),
                 F.max(F.to_date("data_pedido")).alias("ultima_compra")))
    notas = av.groupBy("cliente_id").agg(F.round(F.avg("nota"), 2).alias("nota_media"))
    df = (cli.select("cliente_id", "nome", "estado", "canal_aquisicao", "data_cadastro")
             .join(f, "cliente_id", "left").join(notas, "cliente_id", "left")
             .withColumn("recencia_dias", F.datediff(F.lit(ref), F.col("ultima_compra")))
             .withColumn("segmento",
                         F.when(F.col("qtd_pedidos").isNull(), "sem_compra")
                          .when(F.col("recencia_dias") > 180, "inativo")
                          .when((F.col("qtd_pedidos") >= 5) & (F.col("recencia_dias") <= 60), "campeao")
                          .when(F.col("recencia_dias") <= 90, "ativo")
                          .otherwise("em_risco")))
    df.writeTo("lake.gold.clientes_360").using("iceberg").createOrReplace()
    return spark.table("lake.gold.clientes_360")


def build_avaliacoes_resumo(spark):
    """Satisfação por categoria e mês (alimenta um painel do Metabase)."""
    av = spark.table("lake.silver.avaliacoes")
    pr = spark.table("lake.silver.produtos").select("produto_id", "categoria")
    df = (av.join(pr, "produto_id", "left")
            .groupBy(F.date_format("data_avaliacao", "yyyy-MM").alias("mes"), "categoria")
            .agg(F.count("*").alias("qtd_avaliacoes"), F.round(F.avg("nota"), 2).alias("nota_media"),
                 F.round(100 * F.avg((F.col("nota") <= 2).cast("int")), 1).alias("pct_notas_baixas")))
    df.writeTo("lake.gold.avaliacoes_resumo").using("iceberg").createOrReplace()
    return spark.table("lake.gold.avaliacoes_resumo")
