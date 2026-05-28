"""
Job Curator - Fetches jobs from multiple sources and scores them with Gemini.
Sources: Arbeitnow, Adzuna, direct company career pages.
TTL: extracted from job posting if available, else 7 days (career pages) or 30 days (APIs).
"""

import os
import json
import time
import hashlib
import re
import requests
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
import google.generativeai as genai

# ── CONFIG ────────────────────────────────────────────────────────────────────

GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
ADZUNA_APP_ID  = os.environ["ADZUNA_APP_ID"]
ADZUNA_APP_KEY = os.environ["ADZUNA_APP_KEY"]

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel("gemini-2.0-flash")

TTL_CAREER_PAGE = 7    # days — career pages change fast
TTL_API_DEFAULT = 30   # days — Arbeitnow / Adzuna listings

CANDIDATE_PROFILE = """
Name: Sri Hari Sivashanmugam
Current Role: Data Scientist at Chicago Department of Public Health
Experience: 4+ years production experience
Education: MS Data Science, Illinois Institute of Technology (GPA 3.72); B.Tech CS, Amrita Vishwa Vidyapeetham

Core Skills:
- Data Engineering: PySpark, Apache Spark, Kafka (working knowledge), ETL/ELT, Medallion Lakehouse,
  Azure Databricks, Delta Lake, Unity Catalog, Databricks Asset Bundles, Airflow, Docker, Terraform (prod - AWS migration), Git
- Analytics Engineering: dbt, dimensional modeling, Data Vault, Kimball, normalized schema, Erwin Data Modeler
- ML Platform: MLflow (full lifecycle - experiment tracking, model registry, model serving, deployment)
- Cloud: Azure (ADLS Gen2, Synapse, ADF, Purview, Active Directory, Azure SQL), Databricks, Snowflake,
  AWS (S3, Glue, Athena, Lambda), GCP
- ML/AI: Scikit-learn, Splink (probabilistic record linkage), GLMs, time-series forecasting,
  LLM orchestration, RAG, Google ADK, multi-agent systems
- Languages: Python (advanced), SQL (advanced), PySpark, R, Bash
- Visualization: Tableau, Power BI, Streamlit, Looker (working knowledge - Caterpillar)
- Governance: RBAC, lineage tracking, data quality validation, data catalogs, HIPAA, USCDI, TEFCA
- Statistics: Hypothesis testing, regression, causal inference (foundations), propensity scoring
- Publications: 2 peer-reviewed papers, 39 total citations (employee attrition ML, healthcare content ML)

Target Roles: Data Engineer, Senior Data Engineer, Data Scientist, Senior Data Scientist,
Analytics Engineer, Data Analytics Engineer, ML Engineer (with strong data engineering overlap)

Target Countries: Canada, Germany, Netherlands, and EU member states with straightforward immigration
(Austria, Sweden, Denmark, Ireland, Portugal, Belgium, France)

Immigration: EU Blue Card eligible (IT shortage occupation). Currently H-1B in USA. Requires visa sponsorship or relocation support.

Key Strengths: Cloud migration at scale (3.2M+ records), EMPI/MDM entity resolution, Open Air Chicago
(2nd largest urban air sensor network in the world), MLflow full lifecycle, medallion Lakehouse,
Terraform IaC, stakeholder management to Director level.

Gaps: Deep Kafka/Confluent streaming, Kubernetes/Helm, Java/Scala, deep causal inference, dbt (limited prod depth).
"""

TARGET_COUNTRIES = [
    "germany", "netherlands", "canada", "austria", "sweden", "denmark",
    "ireland", "portugal", "belgium", "france", "europe",
    "berlin", "amsterdam", "toronto", "vancouver", "munich",
    "hamburg", "frankfurt", "dublin", "vienna", "stockholm",
    "copenhagen", "lisbon", "brussels"
]

TARGET_ROLES = [
    "data engineer", "data scientist", "analytics engineer",
    "ml engineer", "machine learning engineer", "data analytics engineer",
    "senior data", "staff data", "principal data", "data analyst"
]

# ── COMPANY CAREER PAGES ──────────────────────────────────────────────────────

COMPANY_PAGES = [
    {"company": "Booking.com",      "url": "https://jobs.booking.com/booking/jobs?location=Amsterdam&department=Data+Science+%26+Analytics", "country": "Netherlands"},
    {"company": "Adyen",            "url": "https://careers.adyen.com/vacancies?team=Data", "country": "Netherlands"},
    {"company": "Catawiki",         "url": "https://www.catawiki.com/en/jobs", "country": "Netherlands"},
    {"company": "Mollie",           "url": "https://boards.greenhouse.io/mollie", "country": "Netherlands"},
    {"company": "Delivery Hero",    "url": "https://careers.deliveryhero.com/jobs?search=data", "country": "Germany"},
    {"company": "Zalando",          "url": "https://jobs.zalando.com/en/jobs/?search=data", "country": "Germany"},
    {"company": "HelloFresh",       "url": "https://careers.hellofresh.com/global/en/search-results?keywords=data", "country": "Germany"},
    {"company": "N26",              "url": "https://n26.com/en-eu/careers/departments/data", "country": "Germany"},
    {"company": "Celonis",          "url": "https://www.celonis.com/careers/jobs/?search=data", "country": "Germany"},
    {"company": "Personio",         "url": "https://www.personio.com/about-personio/careers/?search=data", "country": "Germany"},
    {"company": "Shopify",          "url": "https://www.shopify.com/careers/search?keywords=data&location=Canada", "country": "Canada"},
    {"company": "RBC",              "url": "https://jobs.rbc.com/ca/en/search-results?keywords=data+scientist", "country": "Canada"},
    {"company": "TD Bank",          "url": "https://jobs.td.com/en-CA/jobs/?keyword=data+engineer", "country": "Canada"},
    {"company": "TELUS Health",     "url": "https://careers.telus.com/search?q=data+scientist&location=Canada", "country": "Canada"},
    {"company": "Amazon Canada",    "url": "https://www.amazon.jobs/en/search?base_query=data+engineer&loc_query=Canada", "country": "Canada"},
    {"company": "Microsoft Canada", "url": "https://careers.microsoft.com/us/en/search-results?keywords=data+scientist&location=Canada", "country": "Canada"},
    {"company": "Databricks",       "url": "https://www.databricks.com/company/careers/open-positions", "country": "Europe"},
    {"company": "Spotify",          "url": "https://www.lifeatspotify.com/jobs?l=amsterdam&l=stockholm&q=data", "country": "Europe"},
    {"company": "Optiver",          "url": "https://optiver.com/working-at-optiver/career-opportunities/?search=data", "country": "Netherlands"},
    {"company": "Siemens",          "url": "https://jobs.siemens.com/jobs?search=data+scientist&location=Germany", "country": "Germany"},
]

# ── HELPERS ───────────────────────────────────────────────────────────────────

def job_id(title, company, location):
    raw = f"{title}{company}{location}".lower().strip()
    return hashlib.md5(raw.encode()).hexdigest()[:12]

def load_existing(path="docs/jobs.json"):
    if os.path.exists(path):
        with open(path) as f:
            return {j["id"]: j for j in json.load(f).get("jobs", [])}
    return {}

def is_relevant_role(title):
    title_lower = title.lower()
    return any(role in title_lower for role in TARGET_ROLES)

def is_target_location(location):
    loc_lower = location.lower()
    return any(country in loc_lower for country in TARGET_COUNTRIES)

def extract_deadline_from_text(text):
    """Try to extract an application deadline date from job description text."""
    if not text:
        return None
    patterns = [
        r'apply\s+by\s+(\d{1,2}[\s/\-]\w+[\s/\-]\d{2,4})',
        r'deadline[:\s]+(\d{1,2}[\s/\-]\w+[\s/\-]\d{2,4})',
        r'closing\s+date[:\s]+(\d{1,2}[\s/\-]\w+[\s/\-]\d{2,4})',
        r'applications?\s+close[:\s]+(\d{1,2}[\s/\-]\w+[\s/\-]\d{2,4})',
        r'open\s+until[:\s]+(\d{1,2}[\s/\-]\w+[\s/\-]\d{2,4})',
        r'expires?\s+(\d{4}-\d{2}-\d{2})',
        r'expiry[:\s]+(\d{4}-\d{2}-\d{2})',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            raw = match.group(1).strip()
            for fmt in ("%d %B %Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y",
                        "%d %b %Y", "%B %d, %Y", "%d %B %y"):
                try:
                    return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc).isoformat()
                except ValueError:
                    continue
    return None

def compute_expires_at(description, source, posted_at):
    """Return ISO expiry date: extracted deadline > source-based TTL > default TTL."""
    # 1. Try to extract from description
    deadline = extract_deadline_from_text(description)
    if deadline:
        return deadline, "extracted"

    # 2. Use posted_at + TTL if we have it
    base = None
    if posted_at:
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
            try:
                base = datetime.strptime(posted_at[:19], fmt[:len(posted_at[:19])]).replace(tzinfo=timezone.utc)
                break
            except ValueError:
                continue

    if base is None:
        base = datetime.now(timezone.utc)

    ttl = TTL_CAREER_PAGE if "Career Page" in source else TTL_API_DEFAULT
    return (base + timedelta(days=ttl)).isoformat(), f"{ttl}d-ttl"

def is_expired(job):
    expires_at = job.get("expires_at")
    if not expires_at:
        return False
    try:
        exp = datetime.fromisoformat(expires_at)
        return exp < datetime.now(timezone.utc)
    except Exception:
        return False

# ── GEMINI SCORING ────────────────────────────────────────────────────────────

def score_job(title, company, location, description, visa_info=""):
    prompt = f"""
You are evaluating a job opportunity for the following candidate:

{CANDIDATE_PROFILE}

Job Details:
- Title: {title}
- Company: {company}
- Location: {location}
- Visa/Relocation Info: {visa_info or "Not specified"}
- Description: {description[:3000]}

Respond ONLY with a valid JSON object, no markdown, no explanation:

{{
  "fit_score": <integer 1-10>,
  "visa_eligible": <true|false|"unclear">,
  "relocation_support": <true|false|"unclear">,
  "role_category": "<Data Engineer|Data Scientist|Analytics Engineer|ML Engineer|Other>",
  "match_reasons": ["<reason 1>", "<reason 2>", "<reason 3>"],
  "gaps": ["<gap 1>", "<gap 2>"],
  "one_liner": "<one sentence summary of fit>",
  "apply": <true|false>
}}

Scoring guide:
8-10: Strong fit, apply immediately
6-7: Good fit with minor gaps, worth applying
4-5: Moderate fit, meaningful gaps
1-3: Poor fit

Set apply=true if fit_score >= 6 AND (visa_eligible=true OR relocation_support=true OR visa_eligible="unclear").
"""
    try:
        response = model.generate_content(prompt)
        text = response.text.strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text.strip())
    except Exception as e:
        print(f"  Gemini error: {e}")
        return {
            "fit_score": 0, "visa_eligible": "unclear", "relocation_support": "unclear",
            "role_category": "Other", "match_reasons": [], "gaps": [],
            "one_liner": "Scoring failed", "apply": False
        }

# ── SOURCE 1: ARBEITNOW ───────────────────────────────────────────────────────

def fetch_arbeitnow():
    print("Fetching Arbeitnow...")
    jobs = []
    try:
        resp = requests.get("https://www.arbeitnow.com/api/job-board-api", timeout=15)
        for job in resp.json().get("data", []):
            title    = job.get("title", "")
            company  = job.get("company_name", "")
            location = job.get("location", "")
            desc     = job.get("description", "")
            if not is_relevant_role(title) or not is_target_location(location):
                continue
            jobs.append({
                "title": title, "company": company, "location": location,
                "description": desc, "apply_url": job.get("url", ""),
                "visa_info": "Visa sponsorship available" if job.get("visa_sponsorship") else "",
                "source": "Arbeitnow", "posted_at": job.get("created_at", "")
            })
    except Exception as e:
        print(f"  Arbeitnow error: {e}")
    print(f"  {len(jobs)} relevant jobs")
    return jobs

# ── SOURCE 2: ADZUNA ──────────────────────────────────────────────────────────

def fetch_adzuna(country_code, country_name):
    print(f"Fetching Adzuna ({country_name})...")
    jobs = []
    try:
        url = f"https://api.adzuna.com/v1/api/jobs/{country_code}/search/1"
        params = {
            "app_id": ADZUNA_APP_ID, "app_key": ADZUNA_APP_KEY,
            "results_per_page": 50,
            "what": "data engineer OR data scientist OR analytics engineer OR machine learning engineer",
            "content-type": "application/json", "sort_by": "date"
        }
        resp = requests.get(url, params=params, timeout=15)
        for job in resp.json().get("results", []):
            title = job.get("title", "")
            if not is_relevant_role(title):
                continue
            jobs.append({
                "title": title,
                "company": job.get("company", {}).get("display_name", ""),
                "location": job.get("location", {}).get("display_name", country_name),
                "description": job.get("description", ""),
                "apply_url": job.get("redirect_url", ""),
                "visa_info": "", "source": f"Adzuna ({country_name})",
                "posted_at": job.get("created", "")
            })
    except Exception as e:
        print(f"  Adzuna {country_name} error: {e}")
    print(f"  {len(jobs)} relevant jobs")
    return jobs

# ── SOURCE 3: COMPANY CAREER PAGES ───────────────────────────────────────────

def fetch_company_page(entry):
    company = entry["company"]
    url     = entry["url"]
    country = entry["country"]
    print(f"Fetching {company}...")
    jobs = []
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; JobCurator/1.0)"}
        resp = requests.get(url, headers=headers, timeout=15)
        soup = BeautifulSoup(resp.text, "html.parser")
        seen = set()
        for a in soup.find_all("a", href=True):
            text = a.get_text(strip=True)
            href = a["href"]
            if not text or len(text) < 8 or text in seen:
                continue
            if not is_relevant_role(text):
                continue
            seen.add(text)
            full_url = href if href.startswith("http") else f"https://{url.split('/')[2]}{href}"
            # Try to get posting date from nearby time element
            posted = ""
            parent = a.find_parent()
            if parent:
                t = parent.find("time")
                if t:
                    posted = t.get("datetime", "")
            # Grab surrounding text for deadline extraction
            surrounding = parent.get_text(" ", strip=True)[:500] if parent else ""
            jobs.append({
                "title": text, "company": company, "location": country,
                "description": surrounding,
                "apply_url": full_url, "visa_info": "",
                "source": f"Career Page ({company})",
                "posted_at": posted
            })
        jobs = jobs[:20]
    except Exception as e:
        print(f"  {company} error: {e}")
    print(f"  {len(jobs)} relevant jobs")
    return jobs

# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    os.makedirs("docs", exist_ok=True)
    existing = load_existing()
    print(f"Loaded {len(existing)} existing jobs")

    # Purge expired jobs
    before = len(existing)
    existing = {k: v for k, v in existing.items() if not is_expired(v)}
    purged = before - len(existing)
    if purged:
        print(f"Purged {purged} expired jobs")

    # Gather raw jobs
    raw_jobs = []
    raw_jobs += fetch_arbeitnow()
    for cc, cn in [("de","Germany"),("nl","Netherlands"),("ca","Canada"),("at","Austria"),("se","Sweden"),("ie","Ireland")]:
        raw_jobs += fetch_adzuna(cc, cn)
    for entry in COMPANY_PAGES:
        raw_jobs += fetch_company_page(entry)
        time.sleep(1)

    print(f"\nTotal raw jobs: {len(raw_jobs)}")

    # Score new jobs
    scored = list(existing.values())
    new_count = 0

    for job in raw_jobs:
        jid = job_id(job["title"], job["company"], job["location"])
        if jid in existing:
            continue

        print(f"Scoring: {job['title']} @ {job['company']}")
        gemini_result = score_job(
            job["title"], job["company"], job["location"],
            job["description"], job.get("visa_info", "")
        )
        time.sleep(1)

        expires_at, ttl_source = compute_expires_at(
            job["description"], job["source"], job.get("posted_at", "")
        )

        entry = {
            "id": jid,
            "title": job["title"],
            "company": job["company"],
            "location": job["location"],
            "apply_url": job["apply_url"],
            "source": job["source"],
            "posted_at": job.get("posted_at", ""),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "expires_at": expires_at,
            "ttl_source": ttl_source,
            "status": "new",
            **gemini_result
        }
        scored.append(entry)
        existing[jid] = entry
        new_count += 1

    print(f"\nScored {new_count} new jobs. Total after purge: {len(scored)}")

    # Sort by fit score
    scored.sort(key=lambda x: x.get("fit_score", 0), reverse=True)

    with open("docs/jobs.json", "w") as f:
        json.dump({
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "total": len(scored),
            "purged_this_run": purged,
            "jobs": scored
        }, f, indent=2)

    print("Written to docs/jobs.json")

if __name__ == "__main__":
    main()
