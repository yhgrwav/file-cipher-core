CREATE SCHEMA IF NOT EXISTS cipher;

CREATE TABLE IF NOT EXISTS cipher.chunk_data (
    uuid       UUID        NOT NULL,
    version    INTEGER     NOT NULL,
    ciphertext BYTEA       NOT NULL,
    nonce      BYTEA       NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (uuid, version) -- если прилетает запись с одинаковыми айди и версией - это ошибка (дубликат)
);

CREATE INDEX IF NOT EXISTS idx_chunk_data_uuid_version ON cipher.chunk_data (uuid, version DESC);