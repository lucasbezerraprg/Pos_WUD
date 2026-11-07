from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Dict, List

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
LAKE_ROOT = ROOT / "lakehouse_pandas"
BRONZE_DIR = LAKE_ROOT / "bronze"
SILVER_DIR = LAKE_ROOT / "silver"
GOLD_DIR = LAKE_ROOT / "gold"

TABLES = ("clientes", "produtos", "pedidos", "avaliacoes")
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
CPF_RE = re.compile(r"\b\d{3}\.\d{3}\.\d{3}-\d{2}\b")
PHONE_RE = re.compile(r"\b(?:\+55\s?)?(?:\(?\d{2}\)?\s?)?9?\d{4}-?\d{4}\b")


def _ensure_dirs() -> None:
    for d in (BRONZE_DIR, SILVER_DIR, GOLD_DIR):
        d.mkdir(parents=True, exist_ok=True)


def _parse_mixed_date(series: pd.Series) -> pd.Series:
    s = series.astype("string").str.strip()
    ts = pd.to_datetime(s, errors="coerce", format="ISO8601")
    missing = ts.isna() & s.notna()
    if missing.any():
        ts.loc[missing] = pd.to_datetime(s.loc[missing], errors="coerce", dayfirst=True)
    return ts


def _save_parquet(df: pd.DataFrame, folder: Path, table: str) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    out = folder / f"{table}.parquet"
    df.to_parquet(out, index=False)
    return out


def _load_parquet(folder: Path, table: str) -> pd.DataFrame:
    path = folder / f"{table}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Tabela nao encontrada: {path}")
    return pd.read_parquet(path)


def build_bronze(data_dir: Path = DATA_DIR) -> Dict[str, int]:
    _ensure_dirs()
    ingest_ts = pd.Timestamp.now(tz="UTC").isoformat()
    lotes = sorted([p for p in data_dir.glob("lote*") if p.is_dir()])
    if not lotes:
        raise FileNotFoundError(f"Nenhum lote encontrado em {data_dir}")

    bronze: Dict[str, List[pd.DataFrame]] = {t: [] for t in TABLES}

    for lote_dir in lotes:
        batch_id = lote_dir.name
        for table in TABLES:
            src = lote_dir / ("pedidos.jsonl" if table == "pedidos" else f"{table}.csv")
            if table == "pedidos":
                df = pd.read_json(src, lines=True)
                df["itens"] = df["itens"].apply(lambda x: json.dumps(x, ensure_ascii=False))
            else:
                df = pd.read_csv(src)

            df["_ingest_ts"] = ingest_ts
            df["_source_file"] = str(src)
            df["_batch_id"] = batch_id
            bronze[table].append(df)

    summary: Dict[str, int] = {"lotes": len(lotes)}
    for table, parts in bronze.items():
        full = pd.concat(parts, ignore_index=True)
        _save_parquet(full, BRONZE_DIR, table)
        summary[table] = int(len(full))

    return summary


def load_bronze() -> Dict[str, pd.DataFrame]:
    return {t: _load_parquet(BRONZE_DIR, t) for t in TABLES}


def load_silver() -> Dict[str, pd.DataFrame]:
    return {
        "clientes": _load_parquet(SILVER_DIR, "clientes"),
        "produtos": _load_parquet(SILVER_DIR, "produtos"),
        "pedidos": _load_parquet(SILVER_DIR, "pedidos"),
        "itens_pedido": _load_parquet(SILVER_DIR, "itens_pedido"),
        "avaliacoes": _load_parquet(SILVER_DIR, "avaliacoes"),
        "quarentena": _load_parquet(SILVER_DIR, "quarentena"),
    }


def _new_quarantine(df: pd.DataFrame, origem: str, motivo: str) -> pd.DataFrame:
    base = pd.DataFrame(
        {
            "origem": origem,
            "motivo": motivo,
            "_batch_id": df.get("_batch_id", pd.Series([None] * len(df))),
            "_source_file": df.get("_source_file", pd.Series([None] * len(df))),
        }
    )
    base["registro"] = df.astype("string").fillna("").agg(lambda row: row.to_json(force_ascii=False), axis=1)
    return base[["origem", "motivo", "registro", "_batch_id", "_source_file"]]


def build_silver() -> Dict[str, int]:
    _ensure_dirs()
    brz = load_bronze()
    quar: List[pd.DataFrame] = []

    clientes = brz["clientes"].copy()
    clientes["_ingest_ts"] = pd.to_datetime(clientes["_ingest_ts"], errors="coerce")
    clientes["cliente_id"] = pd.to_numeric(clientes["cliente_id"], errors="coerce").astype("Int64")
    clientes["estado"] = clientes["estado"].astype("string").str.strip().str.upper()
    email = clientes["email"].astype("string").str.strip().str.lower()
    email_ok = email.str.match(EMAIL_RE, na=False)
    clientes["email"] = email.where(email_ok, pd.NA)
    clientes["data_cadastro"] = _parse_mixed_date(clientes["data_cadastro"])

    cli_bad_id = clientes["cliente_id"].isna()
    if cli_bad_id.any():
        quar.append(_new_quarantine(clientes.loc[cli_bad_id], "clientes", "cliente_id_invalido"))

    clientes = (
        clientes.loc[~cli_bad_id]
        .sort_values("_ingest_ts")
        .drop_duplicates(subset=["cliente_id"], keep="last")
        .reset_index(drop=True)
    )

    produtos = brz["produtos"].copy()
    produtos["_ingest_ts"] = pd.to_datetime(produtos["_ingest_ts"], errors="coerce")
    produtos["produto_id"] = pd.to_numeric(produtos["produto_id"], errors="coerce").astype("Int64")
    produtos["preco"] = pd.to_numeric(produtos["preco"], errors="coerce")
    produtos["custo"] = pd.to_numeric(produtos["custo"], errors="coerce")

    prod_invalid = (
        produtos["produto_id"].isna()
        | produtos["preco"].isna()
        | produtos["custo"].isna()
        | (produtos["preco"] <= 0)
        | (produtos["custo"] <= 0)
    )
    if prod_invalid.any():
        quar.append(_new_quarantine(produtos.loc[prod_invalid], "produtos", "valor_invalido"))

    produtos = (
        produtos.loc[~prod_invalid]
        .sort_values("_ingest_ts")
        .drop_duplicates(subset=["produto_id"], keep="last")
        .reset_index(drop=True)
    )

    pedidos = brz["pedidos"].copy()
    pedidos["_ingest_ts"] = pd.to_datetime(pedidos["_ingest_ts"], errors="coerce")
    pedidos["pedido_id"] = pd.to_numeric(pedidos["pedido_id"], errors="coerce").astype("Int64")
    pedidos["cliente_id"] = pd.to_numeric(pedidos["cliente_id"], errors="coerce").astype("Int64")
    pedidos["valor_frete"] = pd.to_numeric(pedidos["valor_frete"], errors="coerce")
    pedidos["data_pedido"] = _parse_mixed_date(pedidos["data_pedido"])
    pedidos = pedidos.sort_values("_ingest_ts").drop_duplicates(subset=["pedido_id"], keep="last")

    cli_ids = set(clientes["cliente_id"].dropna().astype(int).tolist())
    orfao = ~pedidos["cliente_id"].isin(cli_ids)
    if orfao.any():
        quar.append(_new_quarantine(pedidos.loc[orfao], "pedidos", "cliente_orfao"))

    pedidos = pedidos.loc[~orfao].reset_index(drop=True)

    def parse_items(raw: object) -> list:
        if isinstance(raw, list):
            return raw
        if raw is None or (isinstance(raw, float) and pd.isna(raw)):
            return []
        try:
            return json.loads(raw)
        except Exception:
            return []

    pedidos["itens_list"] = pedidos["itens"].apply(parse_items)
    itens = pedidos[["pedido_id", "cliente_id", "data_pedido", "status", "itens_list", "_batch_id", "_source_file"]].explode(
        "itens_list", ignore_index=True
    )
    itens = itens[itens["itens_list"].notna()].copy()
    itens["item_seq"] = itens.groupby("pedido_id").cumcount()

    itens["produto_id"] = pd.to_numeric(itens["itens_list"].apply(lambda x: x.get("produto_id") if isinstance(x, dict) else None), errors="coerce").astype("Int64")
    itens["qtd"] = pd.to_numeric(itens["itens_list"].apply(lambda x: x.get("qtd") if isinstance(x, dict) else None), errors="coerce")
    itens["preco_unit"] = pd.to_numeric(itens["itens_list"].apply(lambda x: x.get("preco_unit") if isinstance(x, dict) else None), errors="coerce")
    itens["valor_item"] = (itens["qtd"] * itens["preco_unit"]).round(2)

    produto_valid_ids = set(produtos["produto_id"].dropna().astype(int).tolist())
    item_valor_invalido = (itens["qtd"].isna()) | (itens["preco_unit"].isna()) | (itens["qtd"] <= 0) | (itens["preco_unit"] <= 0)
    item_prod_invalido = ~itens["produto_id"].isin(produto_valid_ids)

    if item_valor_invalido.any():
        quar.append(_new_quarantine(itens.loc[item_valor_invalido], "itens_pedido", "valor_invalido"))
    if item_prod_invalido.any():
        quar.append(_new_quarantine(itens.loc[item_prod_invalido], "itens_pedido", "produto_inexistente"))

    itens_ok = itens.loc[~(item_valor_invalido | item_prod_invalido)].copy()
    itens_ok = itens_ok.drop(columns=["itens_list"]).drop_duplicates(subset=["pedido_id", "item_seq"], keep="first")

    aval = brz["avaliacoes"].copy()
    aval["_ingest_ts"] = pd.to_datetime(aval["_ingest_ts"], errors="coerce")
    aval["avaliacao_id"] = pd.to_numeric(aval["avaliacao_id"], errors="coerce").astype("Int64")
    aval["pedido_id"] = pd.to_numeric(aval["pedido_id"], errors="coerce").astype("Int64")
    aval["cliente_id"] = pd.to_numeric(aval["cliente_id"], errors="coerce").astype("Int64")
    aval["produto_id"] = pd.to_numeric(aval["produto_id"], errors="coerce").astype("Int64")
    aval["nota"] = pd.to_numeric(aval["nota"], errors="coerce")
    aval["data_avaliacao"] = _parse_mixed_date(aval["data_avaliacao"])

    aval = aval.sort_values("_ingest_ts").drop_duplicates(subset=["avaliacao_id"], keep="last")
    nota_invalida = aval["nota"].isna() | (aval["nota"] < 1) | (aval["nota"] > 5)
    if nota_invalida.any():
        quar.append(_new_quarantine(aval.loc[nota_invalida], "avaliacoes", "nota_invalida"))
    aval = aval.loc[~nota_invalida].reset_index(drop=True)

    quarentena = pd.concat(quar, ignore_index=True) if quar else pd.DataFrame(columns=["origem", "motivo", "registro", "_batch_id", "_source_file"])

    _save_parquet(clientes, SILVER_DIR, "clientes")
    _save_parquet(produtos, SILVER_DIR, "produtos")
    _save_parquet(pedidos.drop(columns=["itens_list"]), SILVER_DIR, "pedidos")
    _save_parquet(itens_ok, SILVER_DIR, "itens_pedido")
    _save_parquet(aval, SILVER_DIR, "avaliacoes")
    _save_parquet(quarentena, SILVER_DIR, "quarentena")

    return {
        "clientes": int(len(clientes)),
        "produtos": int(len(produtos)),
        "pedidos": int(len(pedidos)),
        "itens_pedido": int(len(itens_ok)),
        "avaliacoes": int(len(aval)),
        "quarentena": int(len(quarentena)),
    }


def _mask_pii(text: str) -> str:
    t = text
    t = CPF_RE.sub("[CPF]", t)
    t = PHONE_RE.sub("[TELEFONE]", t)
    t = re.sub(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", "[EMAIL]", t)
    return t


def build_gold() -> Dict[str, int]:
    _ensure_dirs()
    s = load_silver()

    pedidos = s["pedidos"].copy()
    itens = s["itens_pedido"].copy()
    produtos = s["produtos"][["produto_id", "categoria", "custo"]].copy()
    clientes = s["clientes"].copy()
    aval = s["avaliacoes"].copy()

    base = itens.merge(pedidos[["pedido_id", "data_pedido", "status", "cliente_id"]], on=["pedido_id", "cliente_id"], how="left")
    base = base.merge(produtos, on="produto_id", how="left")
    base["data"] = pd.to_datetime(base["data_pedido"], errors="coerce").dt.date
    base["receita"] = base["valor_item"].astype(float)
    base["custo_total"] = (base["qtd"].astype(float) * base["custo"].astype(float)).round(2)

    vendas = (
        base.groupby(["data", "categoria", "status"], dropna=False)
        .agg(qtd_itens=("qtd", "sum"), receita=("receita", "sum"), custo_total=("custo_total", "sum"))
        .reset_index()
    )
    vendas["margem"] = (vendas["receita"] - vendas["custo_total"]).round(2)

    pedido_receita = base.groupby("pedido_id", as_index=False).agg(receita_pedido=("receita", "sum"), data_pedido=("data_pedido", "max"), cliente_id=("cliente_id", "max"))
    c360 = pedido_receita.groupby("cliente_id", as_index=False).agg(
        total_pedidos=("pedido_id", "count"),
        total_receita=("receita_pedido", "sum"),
        ultima_compra=("data_pedido", "max"),
    )
    c360["ticket_medio"] = (c360["total_receita"] / c360["total_pedidos"]).round(2)
    c360 = c360.merge(clientes[["cliente_id", "nome", "cidade", "estado", "canal_aquisicao"]], on="cliente_id", how="left")

    if len(c360) >= 4:
        c360["segmento"] = pd.qcut(c360["total_receita"], q=4, labels=["baixo", "medio", "alto", "premium"], duplicates="drop")
    else:
        c360["segmento"] = "baixo"
    c360["segmento"] = c360["segmento"].astype("string").fillna("baixo")

    ares = aval.merge(produtos[["produto_id", "categoria"]], on="produto_id", how="left")
    avaliacoes_resumo = (
        ares.groupby("categoria", dropna=False)
        .agg(qtd_avaliacoes=("avaliacao_id", "count"), nota_media=("nota", "mean"))
        .reset_index()
    )
    avaliacoes_resumo["nota_media"] = avaliacoes_resumo["nota_media"].round(2)

    chunks = aval[["avaliacao_id", "pedido_id", "cliente_id", "produto_id", "nota", "comentario", "data_avaliacao"]].copy()
    chunks["chunk_text"] = chunks["comentario"].astype("string").fillna("").str.replace(r"\s+", " ", regex=True).str.strip()
    chunks = chunks[chunks["chunk_text"].str.len() >= 20].copy()
    chunks["chunk_text"] = chunks["chunk_text"].map(_mask_pii)
    chunks = chunks.merge(produtos[["produto_id", "categoria"]], on="produto_id", how="left")
    chunks["content_hash"] = chunks["chunk_text"].map(lambda t: hashlib.sha256(t.encode("utf-8")).hexdigest())
    chunks["chunk_id"] = chunks.apply(
        lambda r: hashlib.sha256(f"{r['avaliacao_id']}:{r['content_hash']}".encode("utf-8")).hexdigest()[:20], axis=1
    )
    chunks = chunks[[
        "chunk_id",
        "avaliacao_id",
        "pedido_id",
        "cliente_id",
        "produto_id",
        "categoria",
        "nota",
        "data_avaliacao",
        "chunk_text",
        "content_hash",
    ]]

    _save_parquet(vendas, GOLD_DIR, "vendas_diarias")
    _save_parquet(c360, GOLD_DIR, "clientes_360")
    _save_parquet(avaliacoes_resumo, GOLD_DIR, "avaliacoes_resumo")
    _save_parquet(chunks, GOLD_DIR, "avaliacao_chunks")

    return {
        "vendas_diarias": int(len(vendas)),
        "clientes_360": int(len(c360)),
        "avaliacoes_resumo": int(len(avaliacoes_resumo)),
        "avaliacao_chunks": int(len(chunks)),
    }
