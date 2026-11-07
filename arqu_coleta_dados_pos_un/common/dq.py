"""
dq.py - mini framework de Data Quality (usado no Sábado 1 e no Bootcamp).
Sem dependência externa: cada regra gera uma linha de relatório.
O relatório pode ser gravado numa tabela Iceberg (lake.silver.dq_resultados) = observabilidade.
"""
from datetime import datetime
from pyspark.sql import functions as F


class DQ:
    def __init__(self, spark, df, dataset):
        self.spark, self.df, self.dataset = spark, df, dataset
        self.total = df.count()
        self.results = []

    def _add(self, regra, coluna, falhas, severidade="erro"):
        pct = round(100.0 * falhas / self.total, 2) if self.total else 0.0
        self.results.append((self.dataset, regra, coluna, int(self.total), int(falhas), pct,
                             "OK" if falhas == 0 else severidade.upper(),
                             datetime.now().isoformat(timespec="seconds")))
        return self

    def not_null(self, col, severidade="erro"):
        return self._add("not_null", col, self.df.filter(F.col(col).isNull()).count(), severidade)

    def unique(self, col, severidade="erro"):
        dup = self.df.groupBy(col).count().filter("count > 1").count()
        return self._add("unique", col, dup, severidade)

    def in_set(self, col, valid, severidade="erro"):
        falhas = self.df.filter(~F.col(col).isin(list(valid)) | F.col(col).isNull()).count()
        return self._add(f"in_set({len(valid)} valores)", col, falhas, severidade)

    def between(self, col, lo, hi, severidade="erro"):
        falhas = self.df.filter((F.col(col) < lo) | (F.col(col) > hi)).count()
        return self._add(f"between({lo},{hi})", col, falhas, severidade)

    def regex(self, col, pattern, severidade="aviso"):
        falhas = self.df.filter(~F.col(col).rlike(pattern)).count()
        return self._add(f"regex({pattern})", col, falhas, severidade)

    def custom(self, nome, condicao_falha_sql, severidade="erro"):
        """condicao_falha_sql: expressão SQL verdadeira para as linhas COM problema."""
        return self._add(nome, "-", self.df.filter(condicao_falha_sql).count(), severidade)

    def report(self):
        cols = ["dataset", "regra", "coluna", "total", "falhas", "pct_falha", "status", "executado_em"]
        return self.spark.createDataFrame(self.results, cols)

    def save(self, table="lake.silver.dq_resultados"):
        rep = self.report()
        rep.writeTo(table).using("iceberg").createOrReplace() if not self.spark.catalog.tableExists(table) \
            else rep.writeTo(table).append()
        return rep
