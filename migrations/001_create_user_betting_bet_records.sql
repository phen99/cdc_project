-- ═══════════════════════════════════════════════════════════════════════════
-- Migration: 001_create_user_betting_bet_records.sql
-- Target DB:  Warehouse database (default: kla-klouk-data-warehouse)
--
-- Creates a table that stores every field returned in the `records[]` array
-- of the API endpoint:
--
--     GET /betting/admin/bet-records
--     (kla-klouk-user-betting-service)
--
-- The columns below mirror the JSON keys of the response item
-- (resources.UserBetQueryRowResource).  The `id` and `synced_at` columns are
-- warehouse bookkeeping columns and are NOT part of the API response.
--
-- Apply with:
--     python migrations/run_migrations.py
-- or manually:
--     PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME \
--         -f migrations/001_create_user_betting_bet_records.sql
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS public.user_betting_bet_records (
    -- Warehouse bookkeeping (not part of the API response)
    id                          BIGSERIAL PRIMARY KEY,
    synced_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- ── API response fields ──────────────────────────────────────────────
    bet_ticket_no               VARCHAR(64)  NOT NULL,  -- bet_ticket_no
    bet_at                      TIMESTAMP,               -- bet_at            (YYYY-MM-DD HH:MM:SS)
    settlement_at               TIMESTAMP,               -- settlement_at     ('' → NULL)
    member_account              VARCHAR(255),            -- member_account
    third_party_member_account  VARCHAR(255),            -- third_party_member_account
    game                        VARCHAR(255),            -- game
    currency                    VARCHAR(16),             -- currency
    table_number                VARCHAR(64),             -- table_number
    shoe_no                     BIGINT,                  -- shoe_no
    round_no                    BIGINT,                  -- round_no
    bet_content                 VARCHAR(255),            -- bet_content
    open_result                 VARCHAR(255),            -- open_result        (e.g. 'dragon, tiger, chicken')
    bet_amount                  VARCHAR(32),             -- bet_amount         (formatted string as returned)
    valid_bet                   VARCHAR(32),             -- valid_bet          (formatted string as returned)
    win_loss                    VARCHAR(32),             -- win_loss           ('' when zero)
    status                      VARCHAR(32)              -- status             (Unsettled/Settled/Cancelled/Unknown)
);

-- ── Report-friendly indexes ────────────────────────────────────────────────
-- Natural business key: one row per bet ticket.
CREATE UNIQUE INDEX IF NOT EXISTS uq_user_betting_bet_records_ticket_no
    ON public.user_betting_bet_records (bet_ticket_no);

CREATE INDEX IF NOT EXISTS idx_user_betting_bet_records_bet_at
    ON public.user_betting_bet_records (bet_at);

CREATE INDEX IF NOT EXISTS idx_user_betting_bet_records_member_account
    ON public.user_betting_bet_records (member_account);

CREATE INDEX IF NOT EXISTS idx_user_betting_bet_records_status
    ON public.user_betting_bet_records (status);
