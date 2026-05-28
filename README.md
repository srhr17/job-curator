# Sri Hari — Job Curator

Automated job board that fetches, filters, and scores data roles in Canada, Germany, Netherlands and EU countries — powered by Gemini 2.0 Flash.

## How it works

1. GitHub Actions runs every 6 hours (6am, 12pm, 6pm UTC)
2. Python script pulls jobs from Arbeitnow (EU visa-sponsored), Adzuna (CA/DE/NL/AT), and 20+ company career pages
3. Gemini scores each job against your profile (1-10 fit score, visa status, match reasons, gaps)
4. Results written to `docs/jobs.json`
5. GitHub Pages serves the dashboard at `https://srhr17.github.io/job-curator`

## Setup

### 1. Create the repo
```bash
gh repo create job-curator --public
cd job-curator
git init && git add . && git commit -m "Initial commit"
git remote add origin https://github.com/srhr17/job-curator.git
git push -u origin main
```

### 2. Add GitHub Secrets
Go to repo Settings → Secrets and variables → Actions → New repository secret:

| Secret | Value |
|--------|-------|
| `GEMINI_API_KEY` | Your Gemini API key |
| `ADZUNA_APP_ID` | Your Adzuna App ID |
| `ADZUNA_APP_KEY` | Your Adzuna App Key |

### 3. Enable GitHub Pages
Go to repo Settings → Pages → Source: Deploy from branch → Branch: `main` → Folder: `/docs`

### 4. Run manually first
Go to Actions → Fetch and Score Jobs → Run workflow

Your dashboard will be live at: **https://srhr17.github.io/job-curator**

## Dashboard features

- Filter by country, fit score, role category, visa status
- Search across title, company, skills
- Mark jobs as Applied / Saved / Skipped (persists in browser)
- Status badges auto-update counts
- Expand each card for full match reasons and gaps

## Customization

Edit `scripts/fetch_jobs.py` to:
- Add more company career pages to `COMPANY_PAGES`
- Adjust `TARGET_ROLES` or `TARGET_COUNTRIES`
- Tweak the Gemini scoring prompt in `CANDIDATE_PROFILE`
