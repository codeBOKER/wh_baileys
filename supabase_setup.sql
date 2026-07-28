-- =============================================================
-- WhatsApp Auth Table Setup (Prod + Test)
-- Run this in the Supabase SQL Editor.
-- =============================================================

-- 0. Rename the existing table so MODE=prod uses whatsapp_auth_prod
--    (Skip this if you already renamed it manually)
ALTER TABLE IF EXISTS whatsapp_auth
    RENAME TO whatsapp_auth_prod;

-- 1. Create the test auth table (same schema as whatsapp_auth_prod)
CREATE TABLE IF NOT EXISTS whatsapp_auth_test (
    id TEXT PRIMARY KEY,
    data JSONB NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- 2. Index for fast lookups by id
CREATE INDEX IF NOT EXISTS idx_whatsapp_auth_test_updated_at
    ON whatsapp_auth_test (updated_at);

-- 3. Enable Row Level Security (RLS)
--    The service role key bypasses RLS, so this is just best practice.
ALTER TABLE whatsapp_auth_test ENABLE ROW LEVEL SECURITY;

-- 4. (Optional) Copy existing prod session into test table
--    to start the test number from the same session:
--
--    INSERT INTO whatsapp_auth_test (id, data, updated_at)
--    SELECT id, data, updated_at FROM whatsapp_auth_prod
--    ON CONFLICT (id) DO UPDATE SET
--        data = EXCLUDED.data,
--        updated_at = EXCLUDED.updated_at;
