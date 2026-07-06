\connect warehouse
SET ROLE warehouse;

-- BRONZE: raw payload ใช้ได้ทุก source
CREATE TABLE IF NOT EXISTS bronze.raw_observation (
  batch_id        TEXT,
  source_api      TEXT,
  series_id       TEXT,
  payload         JSONB,
  request_params  JSONB,
  ingested_at     TIMESTAMPTZ DEFAULT now()
);

-- SILVER: 1 แถว = ค่าของ 1 ตัวชี้วัด ณ 1 วัน
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

-- GOLD: SET รายเดือน (set_close = ราคาปิดสิ้นเดือน)
CREATE TABLE IF NOT EXISTS gold.set_monthly (
  obs_month   DATE PRIMARY KEY,
  set_close   NUMERIC,
  set_volume  NUMERIC,
  set_mom     NUMERIC
);

-- GOLD: USD/THB รายเดือน (usdthb = ค่าสิ้นเดือน, ขึ้น = บาทอ่อน)
CREATE TABLE IF NOT EXISTS gold.fx_monthly (
  obs_month   DATE PRIMARY KEY,
  usdthb      NUMERIC,
  usdthb_mom  NUMERIC
);

-- GOLD: policy rate ไทยรายเดือน (BOT + DBnomics)
CREATE TABLE IF NOT EXISTS gold.policy_rate_monthly (
  obs_month    DATE PRIMARY KEY,
  policy_rate  NUMERIC
);

-- GOLD: เงินเฟ้อไทย รายเดือน จาก DBnomics IMF/IFS
CREATE TABLE IF NOT EXISTS gold.cpi_monthly (
  obs_month  DATE PRIMARY KEY,
  cpi_yoy    NUMERIC
);

-- GOLD: พันธบัตรรัฐบาลไทย 10 ปี รายเดือน จาก BOT Statistics (FMRTINTM00296)
CREATE TABLE IF NOT EXISTS gold.bond_monthly (
  obs_month  DATE PRIMARY KEY,
  bond_10y   NUMERIC
);

-- GOLD: Forecast all indicator รวมตารางเดียว
CREATE TABLE IF NOT EXISTS gold.forecast_monthly (
  obs_month     DATE,
  indicator     TEXT,
  yhat          NUMERIC,
  yhat_lower    NUMERIC,
  yhat_upper    NUMERIC,
  is_forecast   BOOLEAN,     -- is_forecast=false = ค่าจริงที่ feed เข้าโมเดล, true = ค่าพยากรณ์ + ช่วงความเชื่อมั่น
  model         TEXT,
  generated_at  TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (indicator, obs_month, is_forecast)
);

-- GOLD: PySpark (test)
CREATE TABLE IF NOT EXISTS gold.set_monthly_spark (
  obs_month   DATE PRIMARY KEY,
  set_close   NUMERIC,
  set_volume  NUMERIC,
  set_mom     NUMERIC
);

-- VIEW FULL JOIN FOR dashboard/CSV
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

---------------------------------------------------------------------------------------------------------------------
-- VIEW: real interest rate = policy_rate - เงินเฟ้อ
CREATE OR REPLACE VIEW gold.real_rate_monthly AS
SELECT obs_month, p.policy_rate, c.cpi_yoy, p.policy_rate - c.cpi_yoy AS real_rate
FROM gold.policy_rate_monthly p
JOIN gold.cpi_monthly c USING (obs_month)
ORDER BY obs_month;

-- VIEW: correlation matrix (long format) ระหว่าง 4 ตัวชี้วัดรายเดือน เฉพาะเดือนที่มีครบทั้ง 4
-- ใช้ % เปลี่ยนแปลง (mom) + อัตรา (rate) เลี่ยง spurious จากราคาดิบที่ trend; n = จำนวนเดือน
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
---------------------------------------------------------------------------------------------------------------------
