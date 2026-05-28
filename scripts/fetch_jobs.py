"""
Job Curator - Fetches jobs from multiple sources and scores them with Gemini.
Sources: Arbeitnow, Adzuna, Jobicy, Remotive, Greenhouse/Lever APIs, Playwright for JS-rendered pages.
TTL: extracted from job posting if available, else 7 days (career pages) or 30 days (APIs).
"""

import os
import json
import time
import hashlib
import re
import requests
from datetime import datetime, timezone, timedelta
from playwright.sync_api import sync_playwright
import google.generativeai as genai

# ── CONFIG ────────────────────────────────────────────────────────────────────

GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
ADZUNA_APP_ID  = os.environ["ADZUNA_APP_ID"]
ADZUNA_APP_KEY = os.environ["ADZUNA_APP_KEY"]

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel("gemini-2.5-flash-lite")

TTL_CAREER_PAGE = 7
TTL_API_DEFAULT = 30

CANDIDATE_PROFILE = """
Name: Sri Hari Sivashanmugam
Current Role: Data Scientist at Chicago Department of Public Health
Experience: 4+ years production experience
Education: MS Data Science, Illinois Institute of Technology (GPA 3.72); B.Tech CS, Amrita Vishwa Vidyapeetham

Core Skills:
- Data Engineering: PySpark, Apache Spark, Kafka (working knowledge), ETL/ELT, Medallion Lakehouse,
  Azure Databricks, Delta Lake, Unity Catalog, Databricks Asset Bundles, Airflow, Docker, Terraform (prod), Git
- Analytics Engineering: dbt, dimensional modeling, Data Vault, Kimball, Erwin Data Modeler
- ML Platform: MLflow (full lifecycle - experiment tracking, model registry, model serving, deployment)
- Cloud: Azure (ADLS Gen2, Synapse, ADF, Purview, Active Directory, Azure SQL), Databricks, Snowflake,
  AWS (S3, Glue, Athena, Lambda), GCP
- ML/AI: Scikit-learn, Splink (probabilistic record linkage), GLMs, time-series forecasting,
  LLM orchestration, RAG, Google ADK, multi-agent systems
- Languages: Python (advanced), SQL (advanced), PySpark, R, Bash
- Visualization: Tableau, Power BI, Streamlit, Looker (working knowledge)
- Governance: RBAC, lineage tracking, data quality validation, data catalogs, HIPAA, USCDI, TEFCA
- Statistics: Hypothesis testing, regression, causal inference (foundations), propensity scoring
- Publications: 2 peer-reviewed papers, 39 total citations

Target Roles: Data Engineer, Senior Data Engineer, Data Scientist, Senior Data Scientist,
Analytics Engineer, Data Analytics Engineer, ML Engineer (data engineering overlap)

Target Countries: Canada, Germany, Netherlands, Austria, Sweden, Denmark, Ireland, Portugal, Belgium, France

Immigration: EU Blue Card eligible. Currently H-1B. Requires visa sponsorship or relocation support.

Strengths: Cloud migration at scale, EMPI/MDM, Open Air Chicago pipeline, MLflow full lifecycle,
medallion Lakehouse, Terraform IaC, stakeholder management to Director level.

Gaps: Deep Kafka/Confluent, Kubernetes/Helm, Java/Scala, deep causal inference, dbt (limited prod depth).
"""

TARGET_COUNTRIES = [
    "germany", "netherlands", "canada", "austria", "sweden", "denmark",
    "ireland", "portugal", "belgium", "france", "europe", "remote",
    "berlin", "amsterdam", "toronto", "vancouver", "munich",
    "hamburg", "frankfurt", "dublin", "vienna", "stockholm",
    "copenhagen", "lisbon", "brussels", "utrecht", "eindhoven"
]

TARGET_ROLES = [
    "data engineer", "data scientist", "analytics engineer",
    "ml engineer", "machine learning engineer", "data analytics engineer",
    "senior data", "staff data", "principal data", "data analyst",
    "machine learning scientist", "mlops"
]

# Companies using Greenhouse
GREENHOUSE_COMPANIES = [
    {"slug": "mollie",          "company": "Mollie",          "country": "Netherlands"},
    {"slug": "catawiki",        "company": "Catawiki",        "country": "Netherlands"},
    {"slug": "personio",        "company": "Personio",        "country": "Germany"},
    {"slug": "n26",             "company": "N26",             "country": "Germany"},
    {"slug": "celonis",         "company": "Celonis",         "country": "Germany"},
    {"slug": "gorillas",        "company": "Gorillas",        "country": "Germany"},
    {"slug": "sumup",           "company": "SumUp",           "country": "Germany"},
    {"slug": "adyen",           "company": "Adyen",           "country": "Netherlands"},
    {"slug": "shopify",         "company": "Shopify",         "country": "Canada"},
    {"slug": "wealthsimple",    "company": "Wealthsimple",    "country": "Canada"},
    {"slug": "1password",       "company": "1Password",       "country": "Canada"},
]

# Companies using Lever
LEVER_COMPANIES = [
    {"slug": "hellofresh",      "company": "HelloFresh",      "country": "Germany"},
    {"slug": "deliveryhero",    "company": "Delivery Hero",   "country": "Germany"},
    {"slug": "numbrs",          "company": "Numbrs",          "country": "Germany"},
]

# ── HELPERS ───────────────────────────────────────────────────────────────────

def job_id(title, company, location=""):
    raw = f"{title}{company}{location}".lower().strip()
    return hashlib.md5(raw.encode()).hexdigest()[:12]

def load_existing(path="docs/jobs.json"):
    if os.path.exists(path):
        with open(path) as f:
            return {j["id"]: j for j in json.load(f).get("jobs", [])}
    return {}

def is_relevant_role(title):
    t = title.lower()
    return any(r in t for r in TARGET_ROLES)

def is_target_location(location):
    loc = location.lower()
    return any(c in loc for c in TARGET_COUNTRIES)

def extract_deadline(text):
    if not text:
        return None
    patterns = [
        r'apply\s+by[:\s]+(\d{1,2}[\s/\-]\w+[\s/\-]\d{2,4})',
        r'deadline[:\s]+(\d{1,2}[\s/\-]\w+[\s/\-]\d{2,4})',
        r'closing\s+date[:\s]+(\d{1,2}[\s/\-]\w+[\s/\-]\d{2,4})',
        r'applications?\s+close[s]?[:\s]+(\d{1,2}[\s/\-]\w+[\s/\-]\d{2,4})',
        r'open\s+until[:\s]+(\d{1,2}[\s/\-]\w+[\s/\-]\d{2,4})',
        r'expires?\s+(\d{4}-\d{2}-\d{2})',
    ]
    for p in patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            raw = m.group(1).strip()
            for fmt in ("%d %B %Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d %b %Y", "%B %d, %Y"):
                try:
                    return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc).isoformat()
                except ValueError:
                    continue
    return None

def compute_expires_at(description, source, posted_at):
    deadline = extract_deadline(description)
    if deadline:
        return deadline, "extracted"

    base = datetime.now(timezone.utc)
    if posted_at:
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
            try:
                base = datetime.strptime(posted_at[:19], fmt[:19]).replace(tzinfo=timezone.utc)
                break
            except ValueError:
                continue

    ttl = TTL_CAREER_PAGE if "Career Page" in source else TTL_API_DEFAULT
    return (base + timedelta(days=ttl)).isoformat(), f"{ttl}d-ttl"

def is_expired(job):
    expires_at = job.get("expires_at")
    if not expires_at:
        return False
    try:
        return datetime.fromisoformat(expires_at) < datetime.now(timezone.utc)
    except Exception:
        return False

# ── GEMINI SCORING ────────────────────────────────────────────────────────────

def score_job(title, company, location, description, visa_info=""):
    prompt = f"""You are evaluating a job for this candidate:

{CANDIDATE_PROFILE}

Job:
- Title: {title}
- Company: {company}
- Location: {location}
- Visa/Relocation: {visa_info or "Not specified"}
- Description: {description[:2500]}

Respond ONLY with valid JSON, no markdown, no explanation:

{{"fit_score":<1-10>,"visa_eligible":<true|false|"unclear">,"relocation_support":<true|false|"unclear">,"role_category":"<Data Engineer|Data Scientist|Analytics Engineer|ML Engineer|Other>","match_reasons":["<r1>","<r2>","<r3>"],"gaps":["<g1>","<g2>"],"one_liner":"<one sentence>","apply":<true|false>}}

8-10: strong fit. 6-7: good fit. 4-5: moderate. 1-3: poor.
apply=true if fit_score>=6 AND visa or relocation likely available."""

    try:
        response = model.generate_content(prompt)
        text = response.text.strip()
        # Strip markdown fences
        if "```" in text:
            parts = text.split("```")
            for part in parts:
                part = part.strip()
                if part.startswith("json"):
                    part = part[4:].strip()
                if part.startswith("{"):
                    text = part
                    break
        # Find JSON object
        start = text.find("{")
        end   = text.rfind("}") + 1
        if start >= 0 and end > start:
            text = text[start:end]
        return json.loads(text)
    except Exception as e:
        print(f"  Gemini error: {e}")
        return {
            "fit_score": 0, "visa_eligible": "unclear", "relocation_support": "unclear",
            "role_category": "Other", "match_reasons": [], "gaps": [],
            "one_liner": "Scoring failed - will retry next run", "apply": False
        }

# ── SOURCE 1: ARBEITNOW ───────────────────────────────────────────────────────

def fetch_arbeitnow():
    print("Fetching Arbeitnow...")
    jobs = []
    try:
        resp = requests.get("https://www.arbeitnow.com/api/job-board-api", timeout=15)
        for job in resp.json().get("data", []):
            title    = job.get("title", "")
            location = job.get("location", "")
            if not is_relevant_role(title) or not is_target_location(location):
                continue
            jobs.append({
                "title": title,
                "company": job.get("company_name", ""),
                "location": location,
                "description": job.get("description", ""),
                "apply_url": job.get("url", ""),
                "visa_info": "Visa sponsorship available" if job.get("visa_sponsorship") else "",
                "source": "Arbeitnow",
                "posted_at": job.get("created_at", "")
            })
    except Exception as e:
        print(f"  Arbeitnow error: {e}")
    print(f"  {len(jobs)} jobs")
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
            "what": "data engineer OR data scientist OR analytics engineer OR machine learning",
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
    print(f"  {len(jobs)} jobs")
    return jobs

# ── SOURCE 3: JOBICY (remote EU/Canada tech roles) ────────────────────────────

def fetch_jobicy():
    print("Fetching Jobicy...")
    jobs = []
    try:
        for tag in ["data-engineer", "data-scientist", "machine-learning"]:
            resp = requests.get(f"https://jobicy.com/api/v2/remote-jobs?count=50&tag={tag}", timeout=15)
            for job in resp.json().get("jobs", []):
                title    = job.get("jobTitle", "")
                location = job.get("jobGeo", "Worldwide")
                if not is_relevant_role(title):
                    continue
                jobs.append({
                    "title": title,
                    "company": job.get("companyName", ""),
                    "location": location,
                    "description": job.get("jobDescription", ""),
                    "apply_url": job.get("url", ""),
                    "visa_info": "", "source": "Jobicy",
                    "posted_at": job.get("pubDate", "")
                })
            time.sleep(0.5)
    except Exception as e:
        print(f"  Jobicy error: {e}")
    print(f"  {len(jobs)} jobs")
    return jobs

# ── SOURCE 4: REMOTIVE ────────────────────────────────────────────────────────

def fetch_remotive():
    print("Fetching Remotive...")
    jobs = []
    try:
        for cat in ["data", "machine-learning"]:
            resp = requests.get(f"https://remotive.com/api/remote-jobs?category={cat}&limit=50", timeout=15)
            for job in resp.json().get("jobs", []):
                title    = job.get("title", "")
                location = job.get("candidate_required_location", "Worldwide")
                if not is_relevant_role(title):
                    continue
                if not any(c in location.lower() for c in ["europe", "germany", "netherlands", "canada", "worldwide", "anywhere"]):
                    continue
                jobs.append({
                    "title": title,
                    "company": job.get("company_name", ""),
                    "location": location,
                    "description": job.get("description", ""),
                    "apply_url": job.get("url", ""),
                    "visa_info": "", "source": "Remotive",
                    "posted_at": job.get("publication_date", "")
                })
            time.sleep(0.5)
    except Exception as e:
        print(f"  Remotive error: {e}")
    print(f"  {len(jobs)} jobs")
    return jobs

# ── SOURCE 5: GREENHOUSE API ──────────────────────────────────────────────────

def fetch_greenhouse(slug, company, country):
    print(f"Fetching Greenhouse ({company})...")
    jobs = []
    try:
        resp = requests.get(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true", timeout=15)
        for job in resp.json().get("jobs", []):
            title    = job.get("title", "")
            location = job.get("location", {}).get("name", country)
            if not is_relevant_role(title):
                continue
            if not is_target_location(location) and not is_target_location(country):
                continue
            desc = ""
            if job.get("content"):
                from html.parser import HTMLParser
                class Strip(HTMLParser):
                    def __init__(self): super().__init__(); self.text = []
                    def handle_data(self, d): self.text.append(d)
                p = Strip(); p.feed(job["content"]); desc = " ".join(p.text)
            jobs.append({
                "title": title, "company": company,
                "location": location or country,
                "description": desc,
                "apply_url": job.get("absolute_url", ""),
                "visa_info": "", "source": f"Greenhouse ({company})",
                "posted_at": job.get("updated_at", "")
            })
    except Exception as e:
        print(f"  Greenhouse {company} error: {e}")
    print(f"  {len(jobs)} jobs")
    return jobs

# ── SOURCE 6: LEVER API ───────────────────────────────────────────────────────

def fetch_lever(slug, company, country):
    print(f"Fetching Lever ({company})...")
    jobs = []
    try:
        resp = requests.get(f"https://api.lever.co/v0/postings/{slug}?mode=json", timeout=15)
        for job in resp.json():
            title    = job.get("text", "")
            location = job.get("categories", {}).get("location", country)
            if not is_relevant_role(title):
                continue
            if not is_target_location(location) and not is_target_location(country):
                continue
            desc = " ".join([
                job.get("descriptionPlain", ""),
                " ".join(l.get("content", "") for l in job.get("lists", []))
            ])
            jobs.append({
                "title": title, "company": company,
                "location": location or country,
                "description": desc,
                "apply_url": job.get("hostedUrl", ""),
                "visa_info": "", "source": f"Lever ({company})",
                "posted_at": str(job.get("createdAt", ""))
            })
    except Exception as e:
        print(f"  Lever {company} error: {e}")
    print(f"  {len(jobs)} jobs")
    return jobs

# ── SOURCE 7: PLAYWRIGHT for JS-rendered career pages ────────────────────────

PLAYWRIGHT_TARGETS = [
    {
        "company": "Booking.com",
        "url": "https://jobs.booking.com/booking/jobs?location=Amsterdam&department=Data+Science+%26+Analytics",
        "country": "Netherlands",
        "job_selector": "a[href*='/booking/jobs/']",
    },
    {
        "company": "Zalando",
        "url": "https://jobs.zalando.com/en/jobs/?search=data+engineer",
        "country": "Germany",
        "job_selector": "a[href*='/jobs/']",
    },
    {
        "company": "Delivery Hero",
        "url": "https://careers.deliveryhero.com/jobs?search=data",
        "country": "Germany",
        "job_selector": "a[href*='/job/']",
    },
    {
        "company": "Optiver",
        "url": "https://optiver.com/working-at-optiver/career-opportunities/?search=data",
        "country": "Netherlands",
        "job_selector": "a[href*='/career-opportunities/']",
    },
    {
        "company": "Databricks",
        "url": "https://www.databricks.com/company/careers/open-positions",
        "country": "Europe",
        "job_selector": "a[href*='/open-positions/']",
    },
]

def fetch_playwright_page(entry, page):
    company  = entry["company"]
    url      = entry["url"]
    country  = entry["country"]
    selector = entry["job_selector"]
    print(f"Playwright: {company}...")
    jobs = []
    try:
        page.goto(url, wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(2000)
        links = page.query_selector_all(selector)
        seen = set()
        for link in links[:30]:
            title    = link.inner_text().strip()
            href     = link.get_attribute("href") or ""
            if not title or title in seen or len(title) < 5:
                continue
            if not is_relevant_role(title):
                continue
            seen.add(title)
            full_url = href if href.startswith("http") else f"https://{url.split('/')[2]}{href}"
            jobs.append({
                "title": title, "company": company, "location": country,
                "description": f"See full description at {full_url}",
                "apply_url": full_url, "visa_info": "",
                "source": f"Career Page ({company})",
                "posted_at": ""
            })
    except Exception as e:
        print(f"  {company} Playwright error: {e}")
    print(f"  {len(jobs)} jobs")
    return jobs

# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    os.makedirs("docs", exist_ok=True)
    existing = load_existing()
    print(f"Loaded {len(existing)} existing jobs")

    # Purge expired
    before = len(existing)
    existing = {k: v for k, v in existing.items() if not is_expired(v)}
    purged = before - len(existing)
    if purged:
        print(f"Purged {purged} expired jobs")

    # Collect raw jobs
    raw_jobs = []
    raw_jobs += fetch_arbeitnow()

    for cc, cn in [("de","Germany"),("nl","Netherlands"),("ca","Canada"),
                   ("at","Austria"),("se","Sweden"),("ie","Ireland")]:
        raw_jobs += fetch_adzuna(cc, cn)

    raw_jobs += fetch_jobicy()
    raw_jobs += fetch_remotive()

    for entry in GREENHOUSE_COMPANIES:
        raw_jobs += fetch_greenhouse(entry["slug"], entry["company"], entry["country"])
        time.sleep(0.5)

    for entry in LEVER_COMPANIES:
        raw_jobs += fetch_lever(entry["slug"], entry["company"], entry["country"])
        time.sleep(0.5)

    # Playwright for JS-rendered pages
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page    = browser.new_page()
        for entry in PLAYWRIGHT_TARGETS:
            raw_jobs += fetch_playwright_page(entry, page)
            time.sleep(1)
        browser.close()

    # Deduplicate raw by id
    seen_ids = set()
    deduped  = []
    for job in raw_jobs:
        jid = job_id(job["title"], job["company"], job.get("location",""))
        if jid not in seen_ids:
            seen_ids.add(jid)
            deduped.append(job)
    raw_jobs = deduped

    print(f"\nTotal raw jobs (deduped): {len(raw_jobs)}")

    # Score new jobs
    scored    = list(existing.values())
    new_count = 0

    for job in raw_jobs:
        jid = job_id(job["title"], job["company"], job.get("location",""))
        if jid in existing:
            continue

        print(f"Scoring: {job['title']} @ {job['company']}")
        result = score_job(
            job["title"], job["company"], job["location"],
            job["description"], job.get("visa_info","")
        )
        time.sleep(1)

        expires_at, ttl_source = compute_expires_at(
            job["description"], job["source"], job.get("posted_at","")
        )

        scored.append({
            "id": jid,
            "title": job["title"],
            "company": job["company"],
            "location": job["location"],
            "apply_url": job["apply_url"],
            "source": job["source"],
            "posted_at": job.get("posted_at",""),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "expires_at": expires_at,
            "ttl_source": ttl_source,
            "status": "new",
            **result
        })
        existing[jid] = scored[-1]
        new_count += 1

    print(f"\nScored {new_count} new jobs. Total: {len(scored)}")
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
