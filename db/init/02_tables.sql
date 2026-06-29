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

-- VIEW: real interest rate = policy_rate - เงินเฟ้อ (เฉพาะเดือนที่มีทั้งคู่)
CREATE OR REPLACE VIEW gold.real_rate_monthly AS
SELECT obs_month, p.policy_rate, c.cpi_yoy, p.policy_rate - c.cpi_yoy AS real_rate
FROM gold.policy_rate_monthly p
JOIN gold.cpi_monthly c USING (obs_month)
ORDER BY obs_month;

-- VIEW รวมทุก mart ตาม obs_month (FULL JOIN — เดือนไหนมีบางตัวก็แสดง) สำหรับ dashboard/CSV
CREATE OR REPLACE VIEW gold.dashboard_monthly AS
SELECT obs_month, s.set_close, s.set_mom, s.set_volume,
       f.usdthb, f.usdthb_mom, p.policy_rate, c.cpi_yoy,
       p.policy_rate - c.cpi_yoy AS real_rate
FROM gold.set_monthly s
FULL JOIN gold.fx_monthly f USING (obs_month)
FULL JOIN gold.policy_rate_monthly p USING (obs_month)
FULL JOIN gold.cpi_monthly c USING (obs_month)
ORDER BY obs_month;
