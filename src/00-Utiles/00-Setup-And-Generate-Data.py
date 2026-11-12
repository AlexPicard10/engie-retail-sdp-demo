# Databricks notebook source
# MAGIC %md
# MAGIC # Setup & generate data — Retail Energy SDP demo
# MAGIC Run this **once** before deploying the pipeline. It creates the schemas + a `landing`
# MAGIC Volume and generates the 5 synthetic raw files **directly into the Volume**, one folder per
# MAGIC source (so Auto Loader can monitor each folder):
# MAGIC
# MAGIC ```
# MAGIC /Volumes/<catalog>/<raw_schema>/landing/
# MAGIC   customers/raw_customers.csv            (10,000 customers)
# MAGIC   consumption/raw_consumption.csv        (~3.65M daily meter readings)
# MAGIC   billing/raw_billing.csv                (120,000 monthly bills)
# MAGIC   tariffs/raw_tariff_plans.csv           (11 tariff plans)
# MAGIC   interactions/raw_interactions.json     (30,000 nested-JSON interactions)
# MAGIC ```
# MAGIC
# MAGIC All data is **synthetic** (Mimesis + NumPy) and bears no relation to any real customer.
# MAGIC Set the widgets at the top, attach to **serverless**, then **Run all**.

# COMMAND ----------

# MAGIC %pip install --quiet polars mimesis numpy
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "alp_demo_catalog", "1. Catalog (CREATE SCHEMA rights)")
dbutils.widgets.text("raw_schema", "engie_retail_en", "2. Raw schema (holds the landing Volume)")
dbutils.widgets.text("schema_sql", "engie_retail_en_sql", "3. Pipeline output schema")
dbutils.widgets.text("n_customers", "10000", "4. Number of customers")
dbutils.widgets.dropdown("mode", "load", ["load", "teardown"], "5. Mode")

CATALOG = dbutils.widgets.get("catalog").strip()
RAW_SCHEMA = dbutils.widgets.get("raw_schema").strip()
SCHEMA_SQL = dbutils.widgets.get("schema_sql").strip()
N_CUSTOMERS = int(dbutils.widgets.get("n_customers").strip())
MODE = dbutils.widgets.get("mode").strip()

VOLUME_NAME = "landing"
LANDING = f"/Volumes/{CATALOG}/{RAW_SCHEMA}/{VOLUME_NAME}"

if not CATALOG:
    raise ValueError("Set the 'catalog' widget to a Unity Catalog you can create a schema in.")

print(f"catalog={CATALOG} · raw_schema={RAW_SCHEMA} · schema_sql={SCHEMA_SQL} · n_customers={N_CUSTOMERS:,} · mode={MODE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generator — synthetic French retail-energy data
# MAGIC Ported from the richer demo's `02_Setup/energy_retail_demo_setup.py`. Deterministic
# MAGIC (`SEED = 42`): consumption/billing are driven by per-customer target profiles so the three
# MAGIC segments stay cleanly separable, and ~5% of customers carry a spike/flatline anomaly.

# COMMAND ----------

import json
import os
import unicodedata
from datetime import date, datetime, timedelta

import numpy as np
import polars as pl
from mimesis import Address, Person
from mimesis.locales import Locale

N_INTERACTIONS = 30_000
END_DATE = date(2025, 12, 31)
START_DATE = END_DATE - timedelta(days=364)  # full year 2025-01-01 .. 2025-12-31
SEED = 42

CUSTOMER_TYPES = ["residential", "commercial"]
CUSTOMER_TYPE_WEIGHTS = [0.85, 0.15]
CONTRACT_TYPES = ["regulated_tariff", "market_offer_fixed", "market_offer_indexed", "green_offer"]
CONTRACT_WEIGHTS = [0.35, 0.25, 0.25, 0.15]
METER_TYPES = ["linky_smart", "legacy_electromechanical", "legacy_electronic"]
METER_WEIGHTS = [0.65, 0.25, 0.10]
REGIONS = [
    "Ile-de-France", "Auvergne-Rhone-Alpes", "Nouvelle-Aquitaine", "Occitanie",
    "Hauts-de-France", "Provence-Alpes-Cote-d-Azur", "Grand-Est", "Pays-de-la-Loire",
    "Bretagne", "Normandie", "Bourgogne-Franche-Comte", "Centre-Val-de-Loire", "Corse",
]
REGION_WEIGHTS = [0.19, 0.12, 0.09, 0.09, 0.09, 0.08, 0.08, 0.06, 0.05, 0.05, 0.04, 0.04, 0.02]
SOUTHERN_REGIONS = {"Provence-Alpes-Cote-d-Azur", "Occitanie", "Corse"}
SUBSCRIBED_POWER_KVA = [3, 6, 9, 12, 15, 18, 24, 30, 36]
POWER_WEIGHTS_RESIDENTIAL = [0.05, 0.35, 0.30, 0.15, 0.08, 0.04, 0.02, 0.005, 0.005]
POWER_WEIGHTS_COMMERCIAL = [0.00, 0.05, 0.10, 0.15, 0.20, 0.20, 0.15, 0.10, 0.05]
DWELLING_TYPES_RESIDENTIAL = ["apartment", "house"]
DWELLING_WEIGHTS_RESIDENTIAL = [0.55, 0.45]
HEATING_TYPES = ["electric", "gas", "heat_pump", "other"]
HEATING_WEIGHTS = [0.40, 0.35, 0.15, 0.10]
HOUSEHOLD_SIZES = [1, 2, 3, 4, 5, 6]
HOUSEHOLD_WEIGHTS = [0.32, 0.30, 0.18, 0.12, 0.05, 0.03]
EV_CHARGER_RATE = 0.12
SOLAR_PANEL_RATE = 0.08
SOUTHERN_SOLAR_BOOST = 1.8
ANOMALY_RATE = 0.05
ANOMALY_TYPES = ["spike", "flatline"]
ANOMALY_TYPE_WEIGHTS = [0.55, 0.45]

TARGET_PROFILES = ["peak_heavy", "seasonal_spiker", "green_saver"]
TARGET_PROFILE_WEIGHTS = [0.34, 0.33, 0.33]
PROFILE_PARAMS = {
    "peak_heavy":      {"base_daily_multiplier": (1.7, 2.4), "seasonal_amplitude": 1.0, "peak_fraction": 0.83},
    "seasonal_spiker": {"base_daily_multiplier": (0.85, 1.30), "seasonal_amplitude": 2.0, "peak_fraction": 0.62},
    "green_saver":     {"base_daily_multiplier": (0.35, 0.55), "seasonal_amplitude": 0.9, "peak_fraction": 0.50},
}
PAYMENT_WEIGHTS_BY_PROFILE = {
    "peak_heavy":      [0.80, 0.10, 0.07, 0.03],
    "seasonal_spiker": [0.80, 0.10, 0.07, 0.03],
    "green_saver":     [0.97, 0.02, 0.005, 0.005],
}
MONTHLY_CONSUMPTION_MULTIPLIER = {
    1: 1.80, 2: 1.65, 3: 1.35, 4: 1.05, 5: 0.85, 6: 0.80,
    7: 0.90, 8: 0.85, 9: 0.90, 10: 1.10, 11: 1.45, 12: 1.75,
}
WINTER_MONTHS = {11, 12, 1, 2}
HEATING_WINTER_BOOST = {"electric": 1.50, "heat_pump": 1.20, "gas": 1.05, "other": 1.10}
PAYMENT_STATUSES = ["paid", "pending", "overdue", "partial"]
RECENT_BILL_PENDING_LIFT = 0.25
TVA_RATE = 0.20

INTERACTION_CHANNELS = ["phone", "email", "online_portal", "mobile_app", "in_person"]
INTERACTION_CHANNEL_WEIGHTS = [0.30, 0.25, 0.25, 0.15, 0.05]
INTERACTION_TYPES = ["complaint", "inquiry", "feedback"]
INTERACTION_TYPE_WEIGHTS = [0.30, 0.45, 0.25]
INTERACTION_CATEGORIES = {
    "complaint": ["outage", "billing_dispute", "meter_issue", "voltage_quality", "estimated_reading"],
    "inquiry": ["tariff_change", "meter_installation", "contract_info", "consumption_question", "moving"],
    "feedback": ["positive_experience", "app_feedback", "service_review", "green_energy_comment", "general"],
}
COMPLAINT_KEYWORDS = {
    "outage": ["power cut", "no electricity", "blackout", "grid failure", "unplanned outage"],
    "billing_dispute": ["overcharged", "wrong amount", "estimated bill", "double billed", "tariff error"],
    "meter_issue": ["faulty meter", "linky problem", "meter not working", "wrong reading", "meter replacement"],
    "voltage_quality": ["voltage drop", "flickering lights", "power surge", "unstable supply", "low voltage"],
    "estimated_reading": ["estimated consumption", "no actual reading", "meter not read", "reading dispute", "consumption spike"],
}
INQUIRY_KEYWORDS = {
    "tariff_change": ["change plan", "switch tariff", "better offer", "heures creuses", "green option"],
    "meter_installation": ["linky installation", "smart meter", "appointment", "meter upgrade", "installation date"],
    "contract_info": ["contract details", "renewal", "commitment period", "termination", "conditions"],
    "consumption_question": ["high consumption", "consumption increase", "energy savings", "consumption history", "peak hours"],
    "moving": ["new address", "transfer contract", "moving out", "connection", "disconnection"],
}
FEEDBACK_KEYWORDS = {
    "positive_experience": ["excellent service", "very satisfied", "quick response", "helpful staff", "great support"],
    "app_feedback": ["easy to use", "app improvement", "mobile app", "user interface", "new feature"],
    "service_review": ["professional", "efficient", "reliable", "good communication", "on time"],
    "green_energy_comment": ["renewable energy", "solar", "green tariff", "carbon footprint", "sustainability"],
    "general": ["overall experience", "recommendation", "loyal customer", "good value", "satisfied"],
}
TEMPLATES_BY_CATEGORY = {
    "outage": [
        "Power outage, this is unacceptable.", "No electricity since {duration}.",
        "Repeated outages in my neighbourhood.", "No power this morning, it's really disruptive.",
        "Sudden outage with no warning, what are we supposed to do?",
    ],
    "billing_dispute": [
        "My bill is abnormally high this month.", "Incorrect billed amount, I want it verified.",
        "I'm disputing this bill, it's far too expensive.", "I was charged twice, this is an error.",
        "A bill of {amount} euros, that's way too much.",
    ],
    "meter_issue": [
        "The Linky meter is out of order.", "My meter display no longer works.",
        "There's an error on my meter reading.", "Display problem on the meter.",
        "The Linky beeps non-stop, please send someone.",
    ],
    "voltage_quality": [
        "My installation keeps suffering micro-outages.", "Unstable voltage, my appliances are suffering.",
        "Frequent power surges.", "Lights flickering constantly.",
    ],
    "estimated_reading": [
        "You made an estimate, I want an actual reading.", "The reading wasn't taken, another estimate again.",
        "Estimate far too high compared to my actual consumption.",
        "Why an estimated reading when my meter is perfectly readable?",
    ],
    "tariff_change": [
        "I'd like to switch to the off-peak hours offer.", "What are your green tariffs?",
        "How do I move to the fixed-rate offer?", "I'd like to compare your offers.",
        "Do you have an offer suited to an electric vehicle?",
    ],
    "meter_installation": [
        "When is the Linky installation scheduled at my home?", "How does the Linky installation work?",
        "I'd like an appointment for the installation.", "How long does the meter installation take?",
        "My Linky hasn't been installed yet.",
    ],
    "contract_info": [
        "What are the terms of my contract?", "How do I terminate my contract?",
        "What is the commitment period?", "Renewing my contract, how do I do it?",
        "Can you send me my general terms and conditions?",
    ],
    "consumption_question": [
        "Can you explain my bill for the month of {month}?", "Why has my consumption gone up?",
        "How can I reduce my consumption?", "What are my consumption peaks?",
        "Can I lower my subscribed power to {power} kVA?",
    ],
    "moving": [
        "I'm moving, how do I transfer my contract?", "Setting up service at my new address?",
        "Closing my contract because I'm moving out.", "How long to activate the meter at my new home?",
        "Move in progress, I want to keep my consumption habits.",
    ],
    "positive_experience": [
        "Top-notch customer service, thank you!", "Very satisfied with my new contract.",
        "Quick response to my request, well done.", "Attentive team, I recommend them.",
    ],
    "app_feedback": [
        "Very handy mobile app.", "The app is clear and intuitive.",
        "A bug in the app to fix, otherwise perfect.", "The app helps me track my consumption.",
    ],
    "service_review": [
        "Quick intervention by the technician, well done.", "Very professional technician, thank you.",
        "Responsive after-sales service.", "Very good contact with the advisor.",
    ],
    "green_energy_comment": [
        "Happy to have switched to the green tariff.", "Great eco-friendly move, I'm staying with you.",
        "Green energy is a real plus.", "I support your energy transition.",
    ],
    "general": [
        "Good value for money on my offer.", "I recommend your service.",
        "Customer for {years} years, still satisfied.", "Overall, I'm happy with your service.",
    ],
}
TEMPLATE_FILLS = {
    "duration": ["this morning", "2h", "5h", "since yesterday"],
    "power": ["6", "9", "12"],
    "amount": ["340", "420", "510"],
    "month": ["January", "February", "March", "December"],
    "years": ["3", "5", "8"],
}
RESOLUTION_ACTIONS = [
    "service_restoration", "bill_adjustment", "meter_replacement",
    "technician_dispatch", "tariff_correction", "escalation", "apology_credit",
]
RESOLUTION_STATUSES = ["resolved", "pending", "escalated"]
DEVICES = ["mobile", "desktop", "tablet"]
BROWSERS = ["chrome", "safari", "firefox", "edge"]

TARIFF_PLANS = [
    {"plan_id": "PLAN-01", "name": "Tarif Bleu Regulee", "peak_rate_eur_kwh": 0.2516, "off_peak_rate_eur_kwh": 0.1568, "standing_charge_eur_month": 12.44, "green_energy_pct": 0, "plan_type": "regulated_tariff"},
    {"plan_id": "PLAN-02", "name": "Offre Verte Essentielle", "peak_rate_eur_kwh": 0.2680, "off_peak_rate_eur_kwh": 0.1650, "standing_charge_eur_month": 11.00, "green_energy_pct": 100, "plan_type": "green_offer"},
    {"plan_id": "PLAN-03", "name": "Offre Fixe 24 Mois", "peak_rate_eur_kwh": 0.2450, "off_peak_rate_eur_kwh": 0.1520, "standing_charge_eur_month": 13.50, "green_energy_pct": 0, "plan_type": "market_offer_fixed"},
    {"plan_id": "PLAN-04", "name": "Offre Indexee Flex", "peak_rate_eur_kwh": 0.2400, "off_peak_rate_eur_kwh": 0.1480, "standing_charge_eur_month": 10.50, "green_energy_pct": 0, "plan_type": "market_offer_indexed"},
    {"plan_id": "PLAN-05", "name": "Offre Verte Premium", "peak_rate_eur_kwh": 0.2800, "off_peak_rate_eur_kwh": 0.1750, "standing_charge_eur_month": 14.00, "green_energy_pct": 100, "plan_type": "green_offer"},
    {"plan_id": "PLAN-06", "name": "Offre Heures Creuses Plus", "peak_rate_eur_kwh": 0.2600, "off_peak_rate_eur_kwh": 0.1350, "standing_charge_eur_month": 12.00, "green_energy_pct": 0, "plan_type": "market_offer_fixed"},
    {"plan_id": "PLAN-07", "name": "Offre Eco Pro", "peak_rate_eur_kwh": 0.2200, "off_peak_rate_eur_kwh": 0.1400, "standing_charge_eur_month": 18.00, "green_energy_pct": 50, "plan_type": "market_offer_fixed"},
    {"plan_id": "PLAN-08", "name": "Offre Solaire", "peak_rate_eur_kwh": 0.2550, "off_peak_rate_eur_kwh": 0.1600, "standing_charge_eur_month": 13.00, "green_energy_pct": 75, "plan_type": "green_offer"},
    # FLAT tariff — single rate (peak == off-peak): the headline at-risk target.
    {"plan_id": "PLAN-09", "name": "Offre Entreprise Standard", "peak_rate_eur_kwh": 0.2100, "off_peak_rate_eur_kwh": 0.2100, "standing_charge_eur_month": 25.00, "green_energy_pct": 0, "plan_type": "market_offer_fixed"},
    {"plan_id": "PLAN-10", "name": "Offre Weekend Avantage", "peak_rate_eur_kwh": 0.2700, "off_peak_rate_eur_kwh": 0.1200, "standing_charge_eur_month": 11.50, "green_energy_pct": 0, "plan_type": "market_offer_indexed"},
    # Near-flat residential tariff (off-peak ~ peak).
    {"plan_id": "PLAN-11", "name": "Tarif Unique Confort", "peak_rate_eur_kwh": 0.2200, "off_peak_rate_eur_kwh": 0.2150, "standing_charge_eur_month": 11.50, "green_energy_pct": 0, "plan_type": "market_offer_fixed"},
]
CONTRACT_TO_PLANS = {
    "regulated_tariff": ["PLAN-01"],
    "market_offer_fixed": ["PLAN-03", "PLAN-06", "PLAN-07", "PLAN-09", "PLAN-11"],
    "market_offer_indexed": ["PLAN-04", "PLAN-10"],
    "green_offer": ["PLAN-02", "PLAN-05", "PLAN-08"],
}


def _seasonal_factor(month, heating_type, region):
    base = MONTHLY_CONSUMPTION_MULTIPLIER[month]
    if month in WINTER_MONTHS:
        base *= HEATING_WINTER_BOOST.get(heating_type, 1.0)
    if region in SOUTHERN_REGIONS:
        if month in WINTER_MONTHS:
            base *= 0.85
        elif month in {6, 7, 8}:
            base *= 1.10
    return base


def gen_tariff_plans() -> pl.DataFrame:
    return pl.DataFrame(TARIFF_PLANS)


def gen_customers(rng: np.random.Generator) -> pl.DataFrame:
    person = Person(locale=Locale.FR, seed=SEED)
    address = Address(locale=Locale.FR, seed=SEED)
    first_names = [person.first_name() for _ in range(N_CUSTOMERS)]
    last_names = [person.last_name() for _ in range(N_CUSTOMERS)]
    domains = ["gmail.com", "orange.fr", "free.fr", "sfr.fr", "laposte.net", "outlook.fr", "yahoo.fr"]
    emails = [f"{fn.lower()}.{ln.lower()}{i}@{rng.choice(domains)}" for i, (fn, ln) in enumerate(zip(first_names, last_names))]
    emails = [unicodedata.normalize("NFD", e).encode("ascii", "ignore").decode("ascii") for e in emails]

    customer_types = rng.choice(CUSTOMER_TYPES, size=N_CUSTOMERS, p=CUSTOMER_TYPE_WEIGHTS).tolist()
    contract_types = rng.choice(CONTRACT_TYPES, size=N_CUSTOMERS, p=CONTRACT_WEIGHTS).tolist()
    tariff_plan_ids = [rng.choice(CONTRACT_TO_PLANS[ct]) for ct in contract_types]

    subscribed_power = []
    for ct in customer_types:
        w = POWER_WEIGHTS_COMMERCIAL if ct == "commercial" else POWER_WEIGHTS_RESIDENTIAL
        subscribed_power.append(int(rng.choice(SUBSCRIBED_POWER_KVA, p=w)))

    meter_types = []
    for ct in customer_types:
        if ct == "commercial":
            meter_types.append("linky_smart" if rng.random() < 0.90 else "legacy_electronic")
        else:
            meter_types.append(rng.choice(METER_TYPES, p=METER_WEIGHTS))

    regions = rng.choice(REGIONS, size=N_CUSTOMERS, p=REGION_WEIGHTS).tolist()
    cities = [address.city() for _ in range(N_CUSTOMERS)]

    signup_start = START_DATE - timedelta(days=1095)
    signup_days = (START_DATE - signup_start).days
    signup_offsets = rng.beta(2, 5, size=N_CUSTOMERS) * signup_days
    signup_dates = [signup_start + timedelta(days=int(d)) for d in signup_offsets]

    dwelling_types = []
    for ct in customer_types:
        if ct == "commercial":
            dwelling_types.append("commercial_premises")
        else:
            dwelling_types.append(rng.choice(DWELLING_TYPES_RESIDENTIAL, p=DWELLING_WEIGHTS_RESIDENTIAL))

    heating_types = rng.choice(HEATING_TYPES, size=N_CUSTOMERS, p=HEATING_WEIGHTS).tolist()

    household_sizes = []
    for ct in customer_types:
        household_sizes.append(0 if ct == "commercial" else int(rng.choice(HOUSEHOLD_SIZES, p=HOUSEHOLD_WEIGHTS)))

    has_ev_charger = []
    for ct, dt, kva in zip(customer_types, dwelling_types, subscribed_power):
        eligible = ct == "residential" and dt == "house" and kva >= 9
        has_ev_charger.append(bool(eligible and rng.random() < EV_CHARGER_RATE))

    has_solar_panels = []
    for ct, dt, region in zip(customer_types, dwelling_types, regions):
        if ct != "residential" or dt != "house":
            has_solar_panels.append(False)
            continue
        rate = SOLAR_PANEL_RATE * (SOUTHERN_SOLAR_BOOST if region in SOUTHERN_REGIONS else 1.0)
        has_solar_panels.append(bool(rng.random() < rate))

    anomaly_draw = rng.random(N_CUSTOMERS)
    anomaly_flag = (anomaly_draw < ANOMALY_RATE).tolist()
    anomaly_type = [str(rng.choice(ANOMALY_TYPES, p=ANOMALY_TYPE_WEIGHTS)) if f else "" for f in anomaly_flag]

    target_profile = []
    for ctype in customer_types:
        if ctype == "commercial":
            target_profile.append("peak_heavy")
        else:
            target_profile.append(str(rng.choice(TARGET_PROFILES, p=TARGET_PROFILE_WEIGHTS)))

    base = rng.beta(1.6, 5.0, size=N_CUSTOMERS)
    days_since_signup = np.array([(START_DATE - sd).days for sd in signup_dates], dtype=float)
    recency_factor = np.clip(1.0 - days_since_signup / 1095.0, 0.0, 1.0)
    heating_risk = np.array([0.10 if h == "electric" else (0.05 if h == "heat_pump" else 0.0) for h in heating_types])
    churn_risk_score = np.round(np.clip(base + 0.15 * recency_factor + heating_risk, 0.0, 1.0), 3).tolist()

    return pl.DataFrame({
        "customer_id": [f"CUST-{i:05d}" for i in range(N_CUSTOMERS)],
        "first_name": first_names, "last_name": last_names, "email": emails,
        "customer_type": customer_types, "contract_type": contract_types,
        "tariff_plan_id": tariff_plan_ids, "subscribed_power_kva": subscribed_power,
        "meter_type": meter_types, "region": regions, "city": cities,
        "signup_date": signup_dates, "dwelling_type": dwelling_types, "heating_type": heating_types,
        "household_size": household_sizes, "has_ev_charger": has_ev_charger,
        "has_solar_panels": has_solar_panels, "anomaly_flag": anomaly_flag, "anomaly_type": anomaly_type,
        "churn_risk_score": churn_risk_score, "target_profile": target_profile,
    })


def gen_consumption(rng: np.random.Generator, customers_df: pl.DataFrame):
    customer_ids = customers_df["customer_id"].to_list()
    customer_types = customers_df["customer_type"].to_list()
    heating_types = customers_df["heating_type"].to_list()
    regions = customers_df["region"].to_list()
    meter_types = customers_df["meter_type"].to_list()
    subscribed_power = customers_df["subscribed_power_kva"].to_list()
    anomaly_flags = customers_df["anomaly_flag"].to_list()
    anomaly_types = customers_df["anomaly_type"].to_list()
    target_profiles = customers_df["target_profile"].to_list()

    base_daily = np.array([power * (3.0 if ct == "commercial" else 1.5) for power, ct in zip(subscribed_power, customer_types)])
    base_daily = base_daily * np.exp(rng.normal(0, 0.25, size=N_CUSTOMERS))
    profile_multiplier = np.array([
        rng.uniform(*PROFILE_PARAMS[p]["base_daily_multiplier"]) if ct == "residential" else 1.0
        for ct, p in zip(customer_types, target_profiles)
    ])
    base_daily = base_daily * profile_multiplier

    all_days = []
    current = START_DATE
    while current <= END_DATE:
        all_days.append(current)
        current += timedelta(days=1)
    n_days = len(all_days)

    seasonal_matrix = np.array([[_seasonal_factor(m, heating_types[c], regions[c]) for m in range(1, 13)] for c in range(N_CUSTOMERS)])
    seasonal_amp = np.array([PROFILE_PARAMS[p]["seasonal_amplitude"] for p in target_profiles])
    seasonal_matrix = 1.0 + (seasonal_matrix - 1.0) * seasonal_amp[:, np.newaxis]

    day_months = np.array([d.month - 1 for d in all_days])
    day_weekdays = np.array([d.weekday() for d in all_days])
    is_weekend = day_weekdays >= 5

    seasonal_per_day = seasonal_matrix[:, day_months]
    weekend_factor = np.ones((N_CUSTOMERS, n_days))
    is_commercial = np.array([ct == "commercial" for ct in customer_types])
    weekend_factor[is_commercial[:, None] & is_weekend[None, :]] = 0.40
    weekend_factor[(~is_commercial[:, None]) & is_weekend[None, :]] = 1.10

    daily_kwh = base_daily[:, np.newaxis] * seasonal_per_day * weekend_factor
    daily_kwh = np.clip(daily_kwh * rng.normal(1.0, 0.05, size=daily_kwh.shape), 0.5, None)

    for c_idx, (flagged, atype) in enumerate(zip(anomaly_flags, anomaly_types)):
        if not flagged:
            continue
        if atype == "spike":
            spike_days = rng.choice(n_days, size=int(rng.integers(2, 6)), replace=False)
            daily_kwh[c_idx, spike_days] *= float(rng.uniform(4.0, 8.0))
        elif atype == "flatline":
            window = int(rng.integers(3, 11))
            start = int(rng.integers(0, max(1, n_days - window)))
            daily_kwh[c_idx, start:start + window] = 0.05
    daily_kwh = np.round(daily_kwh, 3)

    n_rows = N_CUSTOMERS * n_days
    customer_col = np.repeat(customer_ids, n_days).tolist()
    meter_col = np.repeat(meter_types, n_days).tolist()
    day_ts_template = [datetime(d.year, d.month, d.day, 0, 0, 0) for d in all_days]
    timestamps = day_ts_template * N_CUSTOMERS

    combined = pl.DataFrame({
        "reading_id": [f"READ-{i:07d}" for i in range(n_rows)],
        "customer_id": customer_col,
        "timestamp": timestamps,
        "kwh_consumed": daily_kwh.ravel().tolist(),
        "meter_type": meter_col,
    })
    return combined, daily_kwh, all_days


def gen_billing(rng: np.random.Generator, customers_df: pl.DataFrame, daily_kwh: np.ndarray, all_days: list) -> pl.DataFrame:
    customer_ids = customers_df["customer_id"].to_list()
    tariff_plan_ids = customers_df["tariff_plan_id"].to_list()
    target_profiles = customers_df["target_profile"].to_list()
    plan_lookup = {p["plan_id"]: p for p in TARIFF_PLANS}

    monthly_kwh = np.zeros((N_CUSTOMERS, 12))
    for d_idx, d in enumerate(all_days):
        monthly_kwh[:, d.month - 1] += daily_kwh[:, d_idx]

    rows = {k: [] for k in ["bill_id", "customer_id", "billing_period", "billing_start_date", "billing_end_date",
                            "kwh_billed", "peak_kwh", "off_peak_kwh", "amount_ht_eur", "tax_eur", "amount_eur",
                            "payment_status", "due_date", "payment_date"]}
    bill_counter = 0
    for c in range(N_CUSTOMERS):
        plan = plan_lookup[tariff_plan_ids[c]]
        peak_rate, off_peak_rate, standing = plan["peak_rate_eur_kwh"], plan["off_peak_rate_eur_kwh"], plan["standing_charge_eur_month"]
        profile = target_profiles[c]
        peak_frac = float(np.clip(rng.normal(PROFILE_PARAMS[profile]["peak_fraction"], 0.025), 0.30, 0.95))
        base_weights = PAYMENT_WEIGHTS_BY_PROFILE[profile]
        for m in range(12):
            month = m + 1
            kwh = float(monthly_kwh[c, m])
            peak_kwh = round(kwh * peak_frac, 2)
            off_peak_kwh = round(kwh * (1 - peak_frac), 2)
            amount_ht = round(peak_kwh * peak_rate + off_peak_kwh * off_peak_rate + standing, 2)
            tax = round(amount_ht * TVA_RATE, 2)
            amount = round(amount_ht + tax, 2)
            year = START_DATE.year
            billing_start = date(year, month, 1)
            billing_end = date(year, 12, 31) if month == 12 else date(year, month + 1, 1) - timedelta(days=1)
            due = billing_end + timedelta(days=30)
            if month >= 10:
                lift = min(RECENT_BILL_PENDING_LIFT, base_weights[0])
                weights = [base_weights[0] - lift, base_weights[1] + lift, base_weights[2], base_weights[3]]
            else:
                weights = base_weights
            status = rng.choice(PAYMENT_STATUSES, p=weights)
            pay_date = (due - timedelta(days=int(rng.integers(0, 25)))) if status == "paid" else None
            rows["bill_id"].append(f"BILL-{bill_counter:06d}")
            rows["customer_id"].append(customer_ids[c])
            rows["billing_period"].append(f"{year}-{month:02d}")
            rows["billing_start_date"].append(billing_start)
            rows["billing_end_date"].append(billing_end)
            rows["kwh_billed"].append(round(kwh, 2))
            rows["peak_kwh"].append(peak_kwh)
            rows["off_peak_kwh"].append(off_peak_kwh)
            rows["amount_ht_eur"].append(amount_ht)
            rows["tax_eur"].append(tax)
            rows["amount_eur"].append(amount)
            rows["payment_status"].append(status)
            rows["due_date"].append(due)
            rows["payment_date"].append(pay_date)
            bill_counter += 1
    return pl.DataFrame(rows)


def _fill_template(rng: np.random.Generator, template: str) -> str:
    result = template
    for key, values in TEMPLATE_FILLS.items():
        placeholder = "{" + key + "}"
        while placeholder in result:
            result = result.replace(placeholder, rng.choice(values), 1)
    return result


def gen_interactions(rng: np.random.Generator, customers_df: pl.DataFrame) -> list:
    customer_ids = customers_df["customer_id"].to_list()
    customer_types = customers_df["customer_type"].to_list()
    propensity = np.array([{"residential": 1.0, "commercial": 2.5}[ct] for ct in customer_types])
    probs = propensity / propensity.sum()

    all_days = []
    current = START_DATE
    while current <= END_DATE:
        all_days.append(current)
        current += timedelta(days=1)

    records = []
    for i in range(N_INTERACTIONS):
        cust_id = rng.choice(customer_ids, p=probs)
        interaction_type = rng.choice(INTERACTION_TYPES, p=INTERACTION_TYPE_WEIGHTS)
        channel = rng.choice(INTERACTION_CHANNELS, p=INTERACTION_CHANNEL_WEIGHTS)
        day = rng.choice(all_days)
        interaction_dt = datetime(day.year, day.month, day.day, int(rng.integers(8, 20)), int(rng.integers(0, 60)), 0)

        if interaction_type == "complaint":
            cat_options = INTERACTION_CATEGORIES["complaint"]
            priority = rng.choice(["high", "medium", "low"], p=[0.30, 0.50, 0.20])
            kw_map = COMPLAINT_KEYWORDS
            score = round(float(np.clip(rng.normal(2.0, 0.8), 1.0, 5.0)), 1)
            label = "negative" if score < 2.5 else "neutral"
        elif interaction_type == "inquiry":
            cat_options = INTERACTION_CATEGORIES["inquiry"]
            priority = rng.choice(["high", "medium", "low"], p=[0.10, 0.40, 0.50])
            kw_map = INQUIRY_KEYWORDS
            score = round(float(np.clip(rng.normal(3.5, 0.8), 1.0, 5.0)), 1)
            label = "neutral" if score < 4.0 else "positive"
        else:
            cat_options = INTERACTION_CATEGORIES["feedback"]
            priority = rng.choice(["high", "medium", "low"], p=[0.05, 0.25, 0.70])
            kw_map = FEEDBACK_KEYWORDS
            score = round(float(np.clip(rng.normal(3.8, 0.7), 1.0, 5.0)), 1)
            label = "positive" if score >= 3.5 else "neutral"

        subcat = rng.choice(cat_options)
        kw_pool = kw_map.get(subcat, ["question", "help", "info"])
        keywords = rng.choice(kw_pool, size=min(int(rng.integers(2, 5)), len(kw_pool)), replace=False).tolist()
        raw_message = _fill_template(rng, rng.choice(TEMPLATES_BY_CATEGORY[subcat]))

        resolution = None
        if rng.random() < 0.40:
            resolution = {
                "status": rng.choice(RESOLUTION_STATUSES), "action": rng.choice(RESOLUTION_ACTIONS),
                "resolved_by": f"agent_{int(rng.integers(1, 100)):03d}", "resolution_time_hours": int(rng.integers(1, 168)),
            }
        records.append({
            "interaction_id": f"INT-{i:05d}", "customer_id": cust_id,
            "interaction_date": interaction_dt.strftime("%Y-%m-%dT%H:%M:%SZ"), "channel": channel,
            "raw_message": raw_message,
            "interaction": {"type": interaction_type, "priority": priority, "category": subcat},
            "sentiment": {"score": score, "label": label, "keywords": keywords},
            "details": {"resolution": resolution, "metadata": {
                "device": rng.choice(DEVICES), "browser": rng.choice(BROWSERS),
                "session_duration_sec": int(rng.integers(30, 1800))}},
        })
    return records


def bump_churn_with_interactions(customers_df: pl.DataFrame, interactions: list) -> pl.DataFrame:
    complaint_count, negative_count = {}, {}
    for r in interactions:
        cid = r["customer_id"]
        if r["interaction"]["type"] == "complaint":
            complaint_count[cid] = complaint_count.get(cid, 0) + 1
        if r["sentiment"]["label"] == "negative":
            negative_count[cid] = negative_count.get(cid, 0) + 1
    bumped = []
    for cid, score in zip(customers_df["customer_id"].to_list(), customers_df["churn_risk_score"].to_list()):
        cc, nc = complaint_count.get(cid, 0), negative_count.get(cid, 0)
        bump = (0.18 if cc >= 2 else 0.06 if cc == 1 else 0.0) + (0.12 if nc >= 2 else 0.04 if nc == 1 else 0.0)
        bumped.append(round(min(1.0, score + bump), 3))
    return customers_df.with_columns(pl.Series("churn_risk_score", bumped))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load (mode = `load`) — create schemas + Volume, generate and write the 5 files

# COMMAND ----------

if MODE == "load":
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{RAW_SCHEMA}` COMMENT 'Retail energy SDP demo — raw landing data'")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA_SQL}` COMMENT 'Retail energy SDP demo — pipeline output'")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{RAW_SCHEMA}`.`{VOLUME_NAME}` COMMENT 'Raw retail energy files (CSV + JSON)'")
    print(f"✓ schemas + volume ready ({LANDING})")

    rng = np.random.default_rng(SEED)
    print("Generating…")
    tariff_df = gen_tariff_plans()
    customers_df = gen_customers(rng)
    consumption_df, daily_kwh, all_days = gen_consumption(rng, customers_df)
    billing_df = gen_billing(rng, customers_df, daily_kwh, all_days)
    interactions = gen_interactions(rng, customers_df)
    customers_df = bump_churn_with_interactions(customers_df, interactions)

    # One folder per source so Auto Loader monitors each independently.
    targets = {
        "customers": ("raw_customers.csv", customers_df),
        "consumption": ("raw_consumption.csv", consumption_df),
        "billing": ("raw_billing.csv", billing_df),
        "tariffs": ("raw_tariff_plans.csv", tariff_df),
    }
    for folder, (fname, df) in targets.items():
        os.makedirs(f"{LANDING}/{folder}", exist_ok=True)
        df.write_csv(f"{LANDING}/{folder}/{fname}")
        print(f"  → {folder}/{fname} ({df.height:,} rows)")

    os.makedirs(f"{LANDING}/interactions", exist_ok=True)
    with open(f"{LANDING}/interactions/raw_interactions.json", "w") as f:
        for rec in interactions:
            f.write(json.dumps(rec) + "\n")
    print(f"  → interactions/raw_interactions.json ({len(interactions):,} records, NDJSON)")
    print("✅ Load complete.")
else:
    print("mode != load — skipping generation. (Set mode=load to set up.)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

if MODE == "load":
    expected = {
        "customers/raw_customers.csv", "consumption/raw_consumption.csv",
        "billing/raw_billing.csv", "tariffs/raw_tariff_plans.csv",
        "interactions/raw_interactions.json",
    }
    all_ok = True
    for rel in sorted(expected):
        path = f"{LANDING}/{rel}"
        ok = os.path.exists(path)
        all_ok = all_ok and ok
        print(f"  {'✓' if ok else '✗'} {rel}")
    print("\n" + ("✅ Ready — deploy the bundle and run the pipeline (see README)."
                  if all_ok else "⚠️ Some files are missing."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teardown (mode = `teardown`)
# MAGIC Drops every table / view in the pipeline output schema `schema_sql`. **Preserves** the
# MAGIC schemas, the `landing` Volume and the raw files, so you can re-run the pipeline without
# MAGIC regenerating data. (Delete the pipeline itself from the Deployments panel / pipeline UI.)

# COMMAND ----------

if MODE == "teardown":
    dropped = 0
    for row in spark.sql(f"SHOW TABLES IN `{CATALOG}`.`{SCHEMA_SQL}`").collect():
        name = row["tableName"]
        fq = f"`{CATALOG}`.`{SCHEMA_SQL}`.`{name}`"
        for stmt in (f"DROP MATERIALIZED VIEW IF EXISTS {fq}",
                     f"DROP TABLE IF EXISTS {fq}",
                     f"DROP VIEW IF EXISTS {fq}"):
            try:
                spark.sql(stmt)
                break
            except Exception:
                continue
        print(f"  ✓ dropped {name}")
        dropped += 1
    print(f"✅ Teardown complete — {dropped} object(s) dropped. Volume + raw files preserved.")
else:
    print("mode != teardown — skipping.")
