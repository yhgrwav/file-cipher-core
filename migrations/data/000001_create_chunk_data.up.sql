CREATE SCHEMA IF NOT EXISTS cipher;

CREATE TABLE IF NOT EXISTS cipher.chunk_data (
    uuid       UUID        NOT NULL,
    file_id    UUID        NOT NULL,
    version    INTEGER     NOT NULL,
    ciphertext BYTEA       NOT NULL,
    nonce      BYTEA       NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (uuid, version)
);

CREATE INDEX IF NOT EXISTS idx_chunk_data_uuid_version ON cipher.chunk_data (uuid, version DESC);
CREATE INDEX IF NOT EXISTS idx_chunk_data_file_id_uuid ON cipher.chunk_data (file_id, uuid);