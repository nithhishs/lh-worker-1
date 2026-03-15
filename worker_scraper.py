"""
worker_scraper.py — FULL INTELLIGENCE EDITION
───────────────────────────────────────────────
Extracts EVERY possible detail from each job:

JOB DETAILS:
  ✅ Job title / role
  ✅ Job URL
  ✅ Job description (full text)
  ✅ Job type (full-time, part-time, contract, internship)
  ✅ Seniority level
  ✅ Work mode (remote / on-site / hybrid)
  ✅ Posted date + posted time
  ✅ Number of applicants
  ✅ Salary range (if listed)
  ✅ Required skills / keywords
  ✅ Industries

COMPANY DETAILS:
  ✅ Company name
  ✅ Company LinkedIn URL
  ✅ Company website
  ✅ Company size / employee count range
  ✅ Company founded year
  ✅ Company headquarters / location
  ✅ Company type (public, private, nonprofit etc)
  ✅ Company industry
  ✅ Company description
  ✅ Company specialities

JOB POSTER DETAILS:
  ✅ Poster name
  ✅ Poster title / role
  ✅ Poster LinkedIn profile URL
  ✅ Poster profile picture URL
"""

import os
import sys
import re
import json
import time
import random
import hashlib
import logging
import requests
import feedparser
from bs4 import BeautifulSoup
from xml.etree import ElementTree
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────────────────────
ORACLE_URL   = os.environ["ORACLE_URL"]
SECRET       = os.environ["RECEIVER_SECRET"]
KEYWORD      = os.environ.get("KEYWORD", "Software Developer")
LOCATION     = os.environ.get("LOCATION", "India")
STRATEGY     = os.environ.get("STRATEGY", "all")
JOBS_PER_RUN = int(os.environ.get("JOBS_PER_RUN", "100"))
GITHUB_USER  = os.environ.get("GITHUB_USER", "unknown")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
]

KEYWORDS = [
    "Software Developer", "Software Engineer", "Python Developer",
    "React Developer", "Full Stack Developer", "Backend Developer",
    "Frontend Developer", "Java Developer", "Node.js Developer",
    "Data Engineer", "DevOps Engineer", "Cloud Engineer",
    "ML Engineer", "Android Developer", "iOS Developer",
    "Go Developer", "PHP Developer", "TypeScript Developer",
    "Angular Developer", "Vue Developer", "Spring Boot Developer",
    "Django Developer", "FastAPI Developer", "Microservices Engineer",
    "Site Reliability Engineer", "QA Engineer", "Automation Engineer",
    "Data Scientist", "AI Engineer", "Security Engineer",
    "Flutter Developer", "React Native Developer", "Kotlin Developer",
    "Scala Developer", "Ruby Developer", "Rust Developer",
]

CITIES = [
    "Bangalore", "Hyderabad", "Chennai", "Mumbai", "Pune",
    "Delhi", "Noida", "Gurgaon", "Kolkata", "Ahmedabad",
    "Jaipur", "Kochi", "Coimbatore", "Indore", "Nagpur",
    "Trivandrum", "Chandigarh", "Bhubaneswar", "Mysore", "Remote",
]

# ── Empty job template — every field we want ────────────────────────────────────
def empty_job() -> dict:
    return {
        # ── Core ──────────────────────────────────────
        "id":                    None,
        "scraped_at":            datetime.utcnow().isoformat(),
        "source":                None,

        # ── Job Info ──────────────────────────────────
        "job_title":             None,
        "job_url":               None,
        "job_description":       None,
        "job_type":              None,   # Full-time / Part-time / Contract / Internship
        "seniority_level":       None,   # Entry / Mid / Senior / Director / Executive
        "work_mode":             None,   # On-site / Remote / Hybrid
        "posted_date":           None,   # YYYY-MM-DD
        "posted_time":           None,   # HH:MM:SS UTC if available
        "posted_datetime_raw":   None,   # raw string from LinkedIn
        "applicant_count":       None,   # "Over 200 applicants"
        "salary_min":            None,
        "salary_max":            None,
        "salary_currency":       None,
        "salary_period":         None,   # yearly / monthly / hourly
        "required_skills":       None,   # comma-separated
        "industries":            None,   # comma-separated
        "job_functions":         None,   # comma-separated

        # ── Company Info ──────────────────────────────
        "company_name":          None,
        "company_linkedin_url":  None,
        "company_website":       None,
        "company_size":          None,   # "1,001-5,000 employees"
        "company_size_min":      None,   # 1001
        "company_size_max":      None,   # 5000
        "company_founded":       None,   # year
        "company_hq":            None,
        "company_type":          None,   # Public / Private / Non-profit etc
        "company_industry":      None,
        "company_description":   None,
        "company_specialities":  None,

        # ── Job Poster ────────────────────────────────
        "poster_name":           None,
        "poster_title":          None,
        "poster_linkedin_url":   None,
        "poster_image_url":      None,
    }


# ── HTTP helpers ────────────────────────────────────────────────────────────────
def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent":      random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer":         "https://www.linkedin.com/",
        "DNT":             "1",
        "Connection":      "keep-alive",
    })
    return s

def safe_get(session, url, retries=3, delay_range=(2, 5)):
    for attempt in range(retries):
        try:
            time.sleep(random.uniform(*delay_range))
            r = session.get(url, timeout=25)
            if r.status_code == 200:
                return r
            if r.status_code == 429:
                wait = 90 * (attempt + 1)
                log.warning(f"Rate limited → waiting {wait}s")
                time.sleep(wait)
                session.headers["User-Agent"] = random.choice(USER_AGENTS)
                continue
            if r.status_code in (301, 302):
                log.warning(f"Login wall redirect for {url}")
                return None
            log.warning(f"HTTP {r.status_code}")
        except Exception as e:
            log.error(f"Request error attempt {attempt+1}: {e}")
            time.sleep(10)
    return None

def extract_job_id(url: str) -> str:
    parts = url.rstrip("/").split("/")
    for part in reversed(parts):
        clean = part.split("?")[0]
        if clean.isdigit():
            return clean
    return hashlib.md5(url.encode()).hexdigest()[:16]

def dedupe(jobs: list) -> list:
    seen = set()
    out  = []
    for j in jobs:
        if j.get("id") and j["id"] not in seen:
            seen.add(j["id"])
            out.append(j)
    return out

def parse_salary(text: str) -> tuple:
    """Extract salary range from text like '$80,000 - $120,000/yr' """
    if not text:
        return None, None, None, None
    currency = None
    for sym, name in [("$","USD"),("₹","INR"),("€","EUR"),("£","GBP")]:
        if sym in text:
            currency = name
            break
    period = None
    for p in ["yr","year","month","mon","hr","hour"]:
        if p in text.lower():
            period = p
            break
    nums = re.findall(r"[\d,]+", text.replace(",",""))
    nums = [int(n) for n in nums if n.isdigit()]
    return (
        min(nums) if nums else None,
        max(nums) if nums else None,
        currency,
        period,
    )

def parse_employee_count(text: str) -> tuple:
    """Extract min/max from '1,001-5,000 employees'"""
    if not text:
        return None, None
    nums = re.findall(r"[\d,]+", text)
    nums = [int(n.replace(",","")) for n in nums if n.replace(",","").isdigit()]
    if len(nums) >= 2:
        return nums[0], nums[1]
    if len(nums) == 1:
        return nums[0], None
    return None, None


# ════════════════════════════════════════════════════════════════════════════════
# FULL JOB DETAIL EXTRACTOR
# Given a job URL → fetch page → extract every possible field
# ════════════════════════════════════════════════════════════════════════════════
def extract_full_job_details(session, url: str) -> dict:
    """
    Fetches a LinkedIn job page and extracts ALL available fields.
    Returns a fully populated job dict.
    """
    job = empty_job()
    job["job_url"] = url.split("?")[0]
    job["id"]      = extract_job_id(url)

    resp = safe_get(session, url, delay_range=(2, 4))
    if not resp:
        return job

    soup = BeautifulSoup(resp.text, "lxml")

    # ── Job Title ────────────────────────────────────────────────────────────
    for sel in [
        ("h1", {"class": lambda x: x and "top-card-layout__title" in x}),
        ("h1", {"class": lambda x: x and "topcard__title" in x}),
        ("h1", {"class": lambda x: x and "job-title" in x}),
    ]:
        tag = soup.find(sel[0], sel[1])
        if tag:
            job["job_title"] = tag.get_text(strip=True)
            break

    # ── Posted Date + Time ───────────────────────────────────────────────────
    time_tag = soup.find("time")
    if time_tag:
        raw = time_tag.get("datetime", "") or time_tag.get_text(strip=True)
        job["posted_datetime_raw"] = raw
        if "T" in raw:
            parts = raw.split("T")
            job["posted_date"] = parts[0]
            job["posted_time"] = parts[1].split("Z")[0] if len(parts) > 1 else None
        else:
            job["posted_date"] = raw

    # Fallback: look for "Posted X ago" text
    if not job["posted_date"]:
        posted_tag = soup.find("span", class_=lambda x: x and "posted-time-ago" in x)
        if posted_tag:
            job["posted_datetime_raw"] = posted_tag.get_text(strip=True)

    # ── Applicant Count ──────────────────────────────────────────────────────
    for cls in ["num-applicants__caption", "topcard__flavor--metadata"]:
        tag = soup.find("span", class_=lambda x: x and cls in x)
        if tag and ("applicant" in tag.get_text().lower()):
            job["applicant_count"] = tag.get_text(strip=True)
            break

    # ── Job Description ──────────────────────────────────────────────────────
    for cls in ["description__text", "show-more-less-html__markup"]:
        tag = soup.find("div", class_=lambda x: x and cls in x)
        if tag:
            job["job_description"] = tag.get_text(separator="\n", strip=True)
            break

    # ── Job Criteria (seniority, type, industry, function) ──────────────────
    criteria_section = soup.find("ul", class_=lambda x: x and "description__job-criteria-list" in x)
    if criteria_section:
        items = criteria_section.find_all("li")
        for item in items:
            header = item.find("h3")
            value  = item.find("span")
            if not header or not value:
                continue
            key = header.get_text(strip=True).lower()
            val = value.get_text(strip=True)
            if "seniority" in key:
                job["seniority_level"] = val
            elif "employment" in key or "job type" in key:
                job["job_type"] = val
            elif "industry" in key:
                job["industries"] = val
            elif "function" in key:
                job["job_functions"] = val

    # ── Work Mode (Remote / Hybrid / On-site) ───────────────────────────────
    for tag in soup.find_all(["span", "li", "div"]):
        text = tag.get_text(strip=True).lower()
        if text in ("remote", "hybrid", "on-site", "on site"):
            job["work_mode"] = text.title()
            break

    # ── Salary ───────────────────────────────────────────────────────────────
    salary_tag = soup.find(
        ["div","span"],
        class_=lambda x: x and any(k in (x or "") for k in ["salary","compensation","pay"])
    )
    if salary_tag:
        sal_text = salary_tag.get_text(strip=True)
        job["salary_min"], job["salary_max"], job["salary_currency"], job["salary_period"] = parse_salary(sal_text)

    # Also check structured data (JSON-LD)
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if data.get("@type") == "JobPosting":
                # Salary from structured data
                base_salary = data.get("baseSalary", {})
                if base_salary:
                    value = base_salary.get("value", {})
                    job["salary_min"]      = value.get("minValue")
                    job["salary_max"]      = value.get("maxValue")
                    job["salary_currency"] = base_salary.get("currency")
                    job["salary_period"]   = value.get("unitText")

                # Skills
                skills = data.get("skills", "")
                if skills:
                    job["required_skills"] = skills if isinstance(skills, str) else ", ".join(skills)

                # Posted date from structured data (most accurate)
                date_posted = data.get("datePosted", "")
                if date_posted and not job["posted_date"]:
                    job["posted_datetime_raw"] = date_posted
                    if "T" in date_posted:
                        parts = date_posted.split("T")
                        job["posted_date"] = parts[0]
                        job["posted_time"] = parts[1].replace("Z","").replace("+00:00","")
                    else:
                        job["posted_date"] = date_posted

                # Industry
                if not job["industries"]:
                    job["industries"] = data.get("industry", None)
                break
        except Exception:
            pass

    # ── Company Name + URL ───────────────────────────────────────────────────
    company_link = (
        soup.find("a", class_=lambda x: x and "topcard__org-name-link" in x) or
        soup.find("a", class_=lambda x: x and "ember-view" in x and "topcard" in (x or ""))
    )
    if company_link:
        job["company_name"]         = company_link.get_text(strip=True)
        href = company_link.get("href","")
        if "/company/" in href:
            job["company_linkedin_url"] = "https://www.linkedin.com" + href.split("?")[0] if href.startswith("/") else href.split("?")[0]
    else:
        # Fallback company name
        tag = soup.find("span", class_=lambda x: x and "topcard__flavor" in x)
        if tag:
            job["company_name"] = tag.get_text(strip=True)

    # ── Job Poster ───────────────────────────────────────────────────────────
    # LinkedIn sometimes shows the poster's name and profile
    poster_section = soup.find("div", class_=lambda x: x and "hirer-card" in x)
    if not poster_section:
        poster_section = soup.find("div", class_=lambda x: x and "job-poster" in x)
    if not poster_section:
        poster_section = soup.find("div", class_=lambda x: x and "message-the-recruiter" in x)

    if poster_section:
        pname = poster_section.find(["h3","h4","span"], class_=lambda x: x and "name" in (x or ""))
        if pname:
            job["poster_name"] = pname.get_text(strip=True)

        ptitle = poster_section.find("span", class_=lambda x: x and "title" in (x or ""))
        if ptitle:
            job["poster_title"] = ptitle.get_text(strip=True)

        plink = poster_section.find("a", href=lambda x: x and "/in/" in (x or ""))
        if plink:
            href = plink.get("href","")
            job["poster_linkedin_url"] = href.split("?")[0] if href else None

        pimg = poster_section.find("img")
        if pimg:
            job["poster_image_url"] = pimg.get("src") or pimg.get("data-delayed-url")

    # ── Now fetch Company Details Page ───────────────────────────────────────
    if job.get("company_linkedin_url"):
        job = fetch_company_details(session, job)

    return job


# ════════════════════════════════════════════════════════════════════════════════
# COMPANY DETAILS EXTRACTOR
# Fetches the company's LinkedIn page → extracts all company fields
# ════════════════════════════════════════════════════════════════════════════════
def fetch_company_details(session, job: dict) -> dict:
    """
    Fetches LinkedIn company page and fills in:
    - company_website
    - company_size / min / max
    - company_founded
    - company_hq
    - company_type
    - company_industry
    - company_description
    - company_specialities
    """
    url  = job["company_linkedin_url"]
    if not url:
        return job

    # Use /about page for maximum detail
    about_url = url.rstrip("/") + "/about/"
    resp = safe_get(session, about_url, delay_range=(2, 4))
    if not resp:
        return job

    soup = BeautifulSoup(resp.text, "lxml")

    # ── Company description ──────────────────────────────────────────────────
    desc_tag = (
        soup.find("p", class_=lambda x: x and "about-us__description" in x) or
        soup.find("div", class_=lambda x: x and "core-section-container__content" in x) or
        soup.find("p", {"data-test-id": "about-us__description"})
    )
    if desc_tag:
        job["company_description"] = desc_tag.get_text(separator=" ", strip=True)[:1500]

    # ── Company info items ───────────────────────────────────────────────────
    # LinkedIn renders these as dt/dd pairs or labeled items
    info_items = soup.find_all("div", class_=lambda x: x and "about-us__container" in x)

    # Try definition list format
    dts = soup.find_all("dt")
    dds = soup.find_all("dd")
    for dt, dd in zip(dts, dds):
        key = dt.get_text(strip=True).lower()
        val = dd.get_text(strip=True)
        _map_company_field(job, key, val)

    # Try labeled sections format
    labels = soup.find_all(["h3","dt","span"], class_=lambda x: x and "label" in (x or ""))
    for label in labels:
        key = label.get_text(strip=True).lower()
        # Value is usually in the next sibling
        nxt = label.find_next_sibling()
        if nxt:
            val = nxt.get_text(strip=True)
            _map_company_field(job, key, val)

    # Try JSON-LD on company page
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if data.get("@type") == "Organization":
                if not job["company_website"]:
                    job["company_website"] = data.get("url")
                if not job["company_founded"]:
                    job["company_founded"] = data.get("foundingDate")
                if not job["company_hq"]:
                    addr = data.get("address", {})
                    if addr:
                        job["company_hq"] = f"{addr.get('addressLocality','')}, {addr.get('addressCountry','')}".strip(", ")
                if not job["company_description"]:
                    job["company_description"] = data.get("description","")[:1500]
                break
        except Exception:
            pass

    return job


def _map_company_field(job: dict, key: str, val: str):
    """Map a label/value pair to the correct job field."""
    if not val or val == "–":
        return
    k = key.lower()
    if "website" in k or "url" in k:
        job["company_website"]      = val
    elif "size" in k or "employee" in k:
        job["company_size"]         = val
        mn, mx = parse_employee_count(val)
        job["company_size_min"]     = mn
        job["company_size_max"]     = mx
    elif "founded" in k or "established" in k:
        job["company_founded"]      = val
    elif "headquarter" in k or "location" in k or "hq" in k:
        job["company_hq"]           = val
    elif "type" in k and "company" in k:
        job["company_type"]         = val
    elif "industry" in k or "sector" in k:
        job["company_industry"]     = val
    elif "specialit" in k or "specialt" in k:
        job["company_specialities"] = val


# ════════════════════════════════════════════════════════════════════════════════
# STRATEGY 1 — RSS FEED EXPLOSION
# Gets basic job info. Each job then gets full details fetched separately.
# ════════════════════════════════════════════════════════════════════════════════
def get_urls_from_rss(keyword: str, location: str) -> list:
    kw  = requests.utils.quote(keyword)
    loc = requests.utils.quote(location)
    url = (
        f"https://www.linkedin.com/jobs/search?"
        f"keywords={kw}&location={loc}"
        f"&f_TPR=r86400&trk=public_jobs_jobs-search-bar_search-submit"
    )
    urls = []
    try:
        feed = feedparser.parse(url)
        for entry in feed.entries:
            job_url = entry.get("link","")
            if job_url and "/jobs/view/" in job_url:
                urls.append(job_url.split("?")[0])
    except Exception as e:
        log.error(f"[RSS] Error: {e}")
    return urls


def scrape_rss_explosion(session) -> list:
    all_urls = []
    combos   = [(kw, city) for kw in KEYWORDS for city in CITIES]
    random.shuffle(combos)

    for kw, city in combos:
        urls = get_urls_from_rss(kw, city)
        all_urls.extend(urls)
        time.sleep(random.uniform(0.3, 0.8))
        if len(all_urls) >= JOBS_PER_RUN * 3:
            break

    # Dedupe URLs
    all_urls = list(set(all_urls))
    log.info(f"[RSS] {len(all_urls)} unique job URLs")
    return all_urls


# ════════════════════════════════════════════════════════════════════════════════
# STRATEGY 2 — SEARCH EXPLOSION
# ════════════════════════════════════════════════════════════════════════════════
def get_urls_from_search(session, keyword: str, city: str) -> list:
    kw   = requests.utils.quote(keyword)
    loc  = requests.utils.quote(city)
    urls = []
    start = 0

    while True:
        url  = (
            f"https://www.linkedin.com/jobs/search?"
            f"keywords={kw}&location={loc}"
            f"&f_TPR=r86400&sortBy=DD&start={start}"
        )
        resp = safe_get(session, url)
        if not resp:
            break

        soup  = BeautifulSoup(resp.text, "lxml")
        links = soup.find_all("a", href=lambda x: x and "/jobs/view/" in x)
        if not links:
            break

        for link in links:
            href = link["href"].split("?")[0]
            if href not in urls:
                urls.append(href)

        start += 25
        if start >= 200:
            break

    return urls


def scrape_search_explosion(session) -> list:
    all_urls = []
    combos   = [(kw, city) for kw in KEYWORDS for city in CITIES]
    random.shuffle(combos)

    for kw, city in combos:
        urls = get_urls_from_search(session, kw, city)
        log.info(f"[SEARCH] '{kw}' in '{city}' → {len(urls)} URLs")
        all_urls.extend(urls)
        if len(all_urls) >= JOBS_PER_RUN * 3:
            break

    all_urls = list(set(all_urls))
    log.info(f"[SEARCH] {len(all_urls)} unique job URLs")
    return all_urls


# ════════════════════════════════════════════════════════════════════════════════
# STRATEGY 3 — SITEMAP
# ════════════════════════════════════════════════════════════════════════════════
def scrape_sitemap_urls(session, limit=300) -> list:
    resp = safe_get(session, "https://www.linkedin.com/sitemap.xml", delay_range=(1,2))
    if not resp:
        return []

    try:
        root = ElementTree.fromstring(resp.content)
        ns   = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        sitemaps = [l.text for l in root.findall(".//sm:loc", ns) if l.text and "job" in l.text.lower()]
    except:
        return []

    job_urls = []
    random.shuffle(sitemaps)

    for sm_url in sitemaps[:8]:
        r = safe_get(session, sm_url, delay_range=(1,2))
        if not r:
            continue
        try:
            root = ElementTree.fromstring(r.content)
            ns   = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
            urls = [l.text for l in root.findall(".//sm:loc", ns) if l.text and "/jobs/view/" in l.text]
            job_urls.extend(urls)
        except:
            continue
        if len(job_urls) >= limit:
            break

    return list(set(job_urls))[:limit]


# ════════════════════════════════════════════════════════════════════════════════
# MASTER RUNNER — Collect URLs then fetch FULL details for each
# ════════════════════════════════════════════════════════════════════════════════
def run_all_strategies() -> list:
    session = make_session()

    # Step 1: Collect job URLs from all 3 strategies
    log.info("━"*50)
    log.info("PHASE 1: COLLECTING JOB URLs")
    log.info("━"*50)

    rss_urls     = scrape_rss_explosion(session)
    search_urls  = scrape_search_explosion(session)
    sitemap_urls = scrape_sitemap_urls(session)

    all_urls = list(set(rss_urls + search_urls + sitemap_urls))
    random.shuffle(all_urls)
    all_urls = all_urls[:JOBS_PER_RUN]

    log.info(f"Total unique URLs: {len(all_urls)}")
    log.info(f"  RSS:     {len(rss_urls)}")
    log.info(f"  Search:  {len(search_urls)}")
    log.info(f"  Sitemap: {len(sitemap_urls)}")

    # Step 2: Fetch FULL details for every URL
    log.info("━"*50)
    log.info("PHASE 2: FETCHING FULL JOB + COMPANY DETAILS")
    log.info("━"*50)

    jobs = []
    for i, url in enumerate(all_urls, 1):
        log.info(f"[{i}/{len(all_urls)}] {url}")
        job = extract_full_job_details(session, url)
        if job.get("job_title"):
            jobs.append(job)
            log.info(
                f"  ✅ '{job['job_title']}' @ '{job['company_name']}' "
                f"| size={job['company_size']} "
                f"| founded={job['company_founded']} "
                f"| poster={job['poster_name']}"
            )
        else:
            log.warning(f"  ⚠️  Could not extract title, skipping")

    log.info(f"━"*50)
    log.info(f"COMPLETE: {len(jobs)} fully detailed jobs")
    return jobs


# ════════════════════════════════════════════════════════════════════════════════
# SEND TO ORACLE
# ════════════════════════════════════════════════════════════════════════════════
def send_to_oracle(jobs: list):
    payload = {
        "secret":      SECRET,
        "github_user": GITHUB_USER,
        "keyword":     KEYWORD,
        "location":    LOCATION,
        "jobs":        jobs,
    }
    try:
        r = requests.post(
            ORACLE_URL, json=payload, timeout=180,
            headers={"Content-Type": "application/json"}
        )
        if r.status_code == 200:
            log.info(f"📤 Oracle: {r.json()}")
        else:
            raise Exception(f"Oracle {r.status_code}: {r.text[:100]}")
    except Exception as e:
        log.error(f"Send failed: {e}")
        with open("jobs_fallback.json", "w") as f:
            json.dump(jobs, f, indent=2)
        log.info("💾 Fallback saved locally")


# ════════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info(f"🚀 Worker | account={GITHUB_USER} | strategy={STRATEGY}")
    jobs = run_all_strategies()

    if jobs:
        send_to_oracle(jobs)
        log.info(f"✅ Done. {len(jobs)} fully detailed jobs sent.")
    else:
        log.warning("⚠️ No jobs collected this run.")
        sys.exit(0)
