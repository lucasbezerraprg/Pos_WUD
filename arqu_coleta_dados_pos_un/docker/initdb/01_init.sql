-- Executado automaticamente na 1a subida do Postgres.
CREATE EXTENSION IF NOT EXISTS vector;

CREATE SCHEMA IF NOT EXISTS gold;       -- tabelas que o Metabase lê (Spark grava aqui)
CREATE SCHEMA IF NOT EXISTS vetorial;   -- base vetorial (pgvector)

-- Contrato do banco vetorial. Dimensão 384 = modelo multilingual-MiniLM (troque aqui e em common/vetorial.py).
CREATE TABLE IF NOT EXISTS vetorial.avaliacao_chunks (
    chunk_id        TEXT PRIMARY KEY,          -- estável: av-<avaliacao_id>-<chunk_idx>
    avaliacao_id    BIGINT   NOT NULL,
    chunk_idx       INT      NOT NULL DEFAULT 0,
    produto_id      INT,
    produto         TEXT,
    categoria       TEXT,                      -- filtros de metadado
    nota            SMALLINT,
    data_avaliacao  DATE,
    texto           TEXT     NOT NULL,         -- já limpo e com PII mascarada
    content_hash    TEXT     NOT NULL,         -- sha256(texto): decide se o embedding precisa ser refeito
    embedding       vector(384),               -- NULL = pendente
    embedding_model TEXT,
    embedded_at     TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS avaliacao_chunks_categoria ON vetorial.avaliacao_chunks (categoria);
CREATE INDEX IF NOT EXISTS avaliacao_chunks_hash      ON vetorial.avaliacao_chunks (content_hash);
-- O índice HNSW é criado depois de carregar os vetores (common/vetorial.py: create_index).
