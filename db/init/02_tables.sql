\connect warehouse
SET ROLE warehouse;   -- ให้ตารางมี owner = warehouse ไม่ใช่ superuser

-- BRONZE: raw payload ตามที่ landed (append-only, ใช้ได้ทุก source)
CREATE TABLE IF NOT EXISTS bronze.raw_observation (
  batch_id        TEXT,
  source_api      TEXT,            -- 'SET' / 'FRED' / ...
  series_id       TEXT,
  payload         JSONB,
  request_params  JSONB,
  ingested_at     TIMESTAMPTZ DEFAULT now()
);

-- SILVER: long format — 1 แถว = 1 ตัวชี้วัด-วันที่ (SET แตกเป็น 2 indicator)
CREATE TABLE IF NOT EXISTS silver.macro_observation (
  indicator_code   TEXT,
  obs_date         DATE,
  value            NUMERIC,
  unit             TEXT,
  frequency        CHAR(1),
  source           TEXT,
  source_series_id TEXT,
  ingested_at      TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (indicator_code, obs_date)   -- upsert = idempotent
);

CREATE TABLE IF NOT EXISTS silver.indicator_dim (
  indicator_code  TEXT PRIMARY KEY,
  name_th         TEXT,
  name_en         TEXT,
  category        TEXT,
  unit            TEXT,
  frequency       CHAR(1),
  source          TEXT,
  description     TEXT
);

-- GOLD: SET รายเดือน + derived MoM (set_close = ราคาปิดสิ้นเดือน)
CREATE TABLE IF NOT EXISTS gold.set_monthly (
  obs_month   DATE PRIMARY KEY,
  set_close   NUMERIC,   -- close วันสุดท้ายของเดือน
  set_volume  NUMERIC,   -- volume รวมทั้งเดือน
  set_mom     NUMERIC    -- % เปลี่ยนแปลงเทียบเดือนก่อน (derived)
);

-- GOLD: USD/THB รายเดือน (usdthb = ค่าสิ้นเดือน, ขึ้น = บาทอ่อน)
CREATE TABLE IF NOT EXISTS gold.fx_monthly (
  obs_month   DATE PRIMARY KEY,
  usdthb      NUMERIC,   -- ค่าวันสุดท้ายของเดือน
  usdthb_mom  NUMERIC    -- % เปลี่ยนแปลงเทียบเดือนก่อน (derived)
);

-- GOLD: ดอกเบี้ยนโยบายไทยรายเดือน — forward-fill จาก silver (BOT สด + DBnomics backfill)
-- policy rate เป็น step function เปลี่ยนตอน กนง.ประชุม → แต่ละเดือน = ค่าที่มีผล ณ สิ้นเดือน
CREATE TABLE IF NOT EXISTS gold.policy_rate_monthly (
  obs_month    DATE PRIMARY KEY,
  policy_rate  NUMERIC
);

-- GOLD: เงินเฟ้อไทย (CPI % YoY) รายเดือน จาก DBnomics IMF/IFS
CREATE TABLE IF NOT EXISTS gold.cpi_monthly (
  obs_month  DATE PRIMARY KEY,
  cpi_yoy    NUMERIC      -- % เทียบช่วงเดียวกันปีก่อน (headline)
);

-- GOLD: yield พันธบัตรรัฐบาลไทย 10 ปี รายเดือน จาก BOT Statistics (FMRTINTM00296)
CREATE TABLE IF NOT EXISTS gold.bond_monthly (
  obs_month  DATE PRIMARY KEY,
  bond_10y   NUMERIC      -- % ต่อปี, ค่าสิ้นเดือน
);

-- GOLD: ผลพยากรณ์ทุก indicator รวมตารางเดียว (actual + forecast แยกด้วย is_forecast)
-- is_forecast=false = ค่าจริงที่ feed เข้าโมเดล, true = ค่าพยากรณ์ + ช่วงความเชื่อมั่น
CREATE TABLE IF NOT EXISTS gold.forecast_monthly (
  obs_month     DATE,
  indicator     TEXT,        -- canonical code เช่น TH.SET_INDEX, FX.USDTHB
  yhat          NUMERIC,     -- ค่ากลางที่พยากรณ์ (หรือค่าจริงถ้า is_forecast=false)
  yhat_lower    NUMERIC,     -- ขอบล่าง CI (null สำหรับ actual)
  yhat_upper    NUMERIC,     -- ขอบบน CI (null สำหรับ actual)
  is_forecast   BOOLEAN,
  model         TEXT,        -- เช่น 'holt-winters'
  generated_at  TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (indicator, obs_month, is_forecast)
);

-- VIEW: real interest rate = policy_rate - เงินเฟ้อ (เฉพาะเดือนที่มีทั้งคู่)
CREATE OR REPLACE VIEW gold.real_rate_monthly AS
SELECT obs_month, p.policy_rate, c.cpi_yoy, p.policy_rate - c.cpi_yoy AS real_rate
FROM gold.policy_rate_monthly p
JOIN gold.cpi_monthly c USING (obs_month)
ORDER BY obs_month;

-- VIEW: correlation matrix (long format) ระหว่าง 4 ตัวชี้วัดรายเดือน เฉพาะเดือนที่มีครบทั้ง 4
-- ใช้ % เปลี่ยนแปลง (mom) + อัตรา (rate) เลี่ยง spurious จากราคาดิบที่ trend; n = จำนวนเดือน (เชื่อได้แค่ไหน)
-- ponytail: ใช้ Postgres corr() aggregate เป็น view — ไม่ต้องมี DAG/pandas, คำนวณสดทุก query
CREATE OR REPLACE VIEW gold.correlation_matrix AS
WITH d AS (
  SELECT set_mom, usdthb_mom, policy_rate, cpi_yoy
  FROM gold.dashboard_monthly
  WHERE set_mom IS NOT NULL AND usdthb_mom IS NOT NULL
    AND policy_rate IS NOT NULL AND cpi_yoy IS NOT NULL
)
SELECT 'set_mom' AS var1, 'usdthb_mom' AS var2, round(corr(set_mom, usdthb_mom)::numeric, 3) AS correlation, count(*) AS n FROM d
UNION ALL SELECT 'set_mom', 'policy_rate', round(corr(set_mom, policy_rate)::numeric, 3), count(*) FROM d
UNION ALL SELECT 'set_mom', 'cpi_yoy', round(corr(set_mom, cpi_yoy)::numeric, 3), count(*) FROM d
UNION ALL SELECT 'usdthb_mom', 'policy_rate', round(corr(usdthb_mom, policy_rate)::numeric, 3), count(*) FROM d
UNION ALL SELECT 'usdthb_mom', 'cpi_yoy', round(corr(usdthb_mom, cpi_yoy)::numeric, 3), count(*) FROM d
UNION ALL SELECT 'policy_rate', 'cpi_yoy', round(corr(policy_rate, cpi_yoy)::numeric, 3), count(*) FROM d
-- bond 10Y เพิ่งเริ่มมี ~2024-07 (n น้อย) → คำนวณ pairwise เอง (corr ข้าม null คู่ให้อยู่แล้ว) ไม่รวมใน d
UNION ALL SELECT 'bond_10y', 'policy_rate', round(corr(bond_10y, policy_rate)::numeric, 3), count(*)
  FROM gold.dashboard_monthly WHERE bond_10y IS NOT NULL AND policy_rate IS NOT NULL
UNION ALL SELECT 'bond_10y', 'cpi_yoy', round(corr(bond_10y, cpi_yoy)::numeric, 3), count(*)
  FROM gold.dashboard_monthly WHERE bond_10y IS NOT NULL AND cpi_yoy IS NOT NULL
UNION ALL SELECT 'bond_10y', 'usdthb_mom', round(corr(bond_10y, usdthb_mom)::numeric, 3), count(*)
  FROM gold.dashboard_monthly WHERE bond_10y IS NOT NULL AND usdthb_mom IS NOT NULL;

-- VIEW รวมทุก mart ตาม obs_month (FULL JOIN — เดือนไหนมีบางตัวก็แสดง) สำหรับ dashboard/CSV
CREATE OR REPLACE VIEW gold.dashboard_monthly AS
SELECT obs_month, s.set_close, s.set_mom, s.set_volume,
       f.usdthb, f.usdthb_mom, p.policy_rate, c.cpi_yoy, b.bond_10y,
       p.policy_rate - c.cpi_yoy AS real_rate
FROM gold.set_monthly s
FULL JOIN gold.fx_monthly f USING (obs_month)
FULL JOIN gold.policy_rate_monthly p USING (obs_month)
FULL JOIN gold.cpi_monthly c USING (obs_month)
FULL JOIN gold.bond_monthly b USING (obs_month)
ORDER BY obs_month;
