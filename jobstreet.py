"""
JobStreet Collector — Primary data source for Malaysia and Singapore.

JobStreet (jobstreet.com.my / jobstreet.com.sg) is the dominant job platform
in SEA and our highest-priority collection target. This collector uses
JobStreet's public search pages with structured scraping.

Technical approach:
- JobStreet renders job listings server-side (HTML), making BeautifulSoup sufficient
  for listing pages. Individual posting detail pages may require Playwright for
  full description rendering.
- We collect listing metadata from search pages first, then fetch details
  for each posting in a controlled, rate-limited loop.
- We focus on tech-related job categories to keep the dataset relevant.

Ethical scraping practices followed:
- Respects rate limits (2+ second delay between requests)
- Uses realistic User-Agent headers
- Does not log in or bypass authentication
- Only collects publicly visible data
- Stores minimal PII (no personal applicant data)
"""

import re
import json
import hashlib
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

from src.collectors.base import BaseCollector, RawJobPosting
from src.storage.models import DataSource, CountryCode
from src.utils.logger import get_logger

logger = get_logger(__name__)


# JobStreet search configuration per country
JOBSTREET_CONFIG = {
    CountryCode.MALAYSIA: {
        "base_url": "https://www.jobstreet.com.my",
        "search_url": "https://www.jobstreet.com.my/jobs",
        "currency": "MYR",
    },
    CountryCode.SINGAPORE: {
        "base_url": "https://www.jobstreet.com.sg",
        "search_url": "https://www.jobstreet.com.sg/jobs",
        "currency": "SGD",
    },
}

# Tech job categories we're interested in
TECH_JOB_CATEGORIES = [
    "data-scientist",
    "data-analyst",
    "machine-learning",
    "software-engineer",
    "data-engineer",
    "business-intelligence",
    "artificial-intelligence",
    "devops",
    "cloud-computing",
    "it-project-manager",
]

# Maximum pages to collect per category per run (each page has ~30 listings)
MAX_PAGES_PER_CATEGORY = 5


class JobStreetCollector(BaseCollector):
    """
    Collector for JobStreet Malaysia and Singapore.
    
    Collection strategy:
    1. Iterate through tech job categories
    2. For each category, collect listing pages (title, company, location, meta)
    3. For each listing, fetch the detail page to get full description + requirements
    4. Return all collected postings as RawJobPosting objects
    """
    
    source = DataSource.JOBSTREET
    
    def collect(
        self,
        country: CountryCode,
        categories: Optional[List[str]] = None,
        max_pages: int = MAX_PAGES_PER_CATEGORY,
    ) -> List[RawJobPosting]:
        """
        Main collection method. Iterates through job categories and pages.
        
        Args:
            country: Which country's JobStreet site to scrape (MY or SG)
            categories: Override default tech categories (useful for targeted runs)
            max_pages: Max pages per category (default 5 = ~150 listings per category)
        
        Returns:
            List[RawJobPosting]: All collected postings for this run
        """
        if country not in JOBSTREET_CONFIG:
            logger.error(f"JobStreet not configured for country: {country.value}")
            return []
        
        config = JOBSTREET_CONFIG[country]
        target_categories = categories or TECH_JOB_CATEGORIES
        all_postings = []
        seen_job_ids = set()  # In-run deduplication
        
        logger.info(
            f"JobStreet collection starting: country={country.value}, "
            f"categories={len(target_categories)}, max_pages={max_pages}"
        )
        
        for category in target_categories:
            logger.info(f"Collecting category: {category}")
            category_postings = self._collect_category(
                config=config,
                country=country,
                category=category,
                max_pages=max_pages,
                seen_job_ids=seen_job_ids,
            )
            all_postings.extend(category_postings)
            logger.info(f"Category '{category}': {len(category_postings)} new postings collected")
        
        logger.info(
            f"JobStreet collection complete: country={country.value}, "
            f"total={len(all_postings)} postings"
        )
        return all_postings
    
    def _collect_category(
        self,
        config: Dict,
        country: CountryCode,
        category: str,
        max_pages: int,
        seen_job_ids: set,
    ) -> List[RawJobPosting]:
        """Collects all postings for a single job category across multiple pages."""
        postings = []
        
        for page_num in range(1, max_pages + 1):
            page_url = self._build_search_url(config, category, page_num)
            logger.debug(f"Fetching page {page_num}: {page_url}")
            
            try:
                listing_items = self._scrape_listing_page(page_url)
                
                if not listing_items:
                    logger.info(f"No more results at page {page_num} for category '{category}'")
                    break
                
                for item in listing_items:
                    job_id = item.get("job_id", "")
                    
                    # Skip if we've already seen this job in this run
                    if job_id in seen_job_ids:
                        continue
                    seen_job_ids.add(job_id)
                    
                    # Fetch full details for this posting
                    try:
                        detail_url = item.get("url", "")
                        if detail_url:
                            detail = self._scrape_detail_page(detail_url)
                            item.update(detail)
                    except Exception as e:
                        logger.warning(f"Failed to fetch detail for job {job_id}: {e}")
                        # We still proceed with listing-level data
                    
                    posting = self._map_to_raw_posting(item, country, config)
                    if posting:
                        postings.append(posting)
                
                logger.debug(f"Page {page_num}: {len(listing_items)} listings found")
                
            except Exception as e:
                logger.error(f"Failed to scrape page {page_num} for '{category}': {e}")
                continue  # Try next page instead of failing completely
        
        return postings
    
    def _build_search_url(self, config: Dict, category: str, page: int) -> str:
        """Constructs the search URL for a given category and page number."""
        base = config["search_url"]
        # JobStreet URL format: /jobs/{category}?page={n}
        return f"{base}/{category}?page={page}&sortMode=ListedDate"
    
    def _scrape_listing_page(self, url: str) -> List[Dict[str, Any]]:
        """
        Scrapes a JobStreet search results page.
        Returns a list of job listing metadata dicts.
        
        JobStreet injects job data as JSON in a __NEXT_DATA__ script tag,
        which is more reliable than parsing HTML elements directly.
        """
        response = self._get(url)
        soup = BeautifulSoup(response.text, "lxml")
        
        # JobStreet (Next.js app) embeds structured data in __NEXT_DATA__
        next_data_tag = soup.find("script", {"id": "__NEXT_DATA__"})
        
        if next_data_tag:
            return self._parse_next_data(next_data_tag.string)
        
        # Fallback: parse HTML directly if __NEXT_DATA__ not found
        logger.debug("__NEXT_DATA__ not found, falling back to HTML parsing")
        return self._parse_html_listings(soup)
    
    def _parse_next_data(self, json_string: str) -> List[Dict[str, Any]]:
        """
        Parses the __NEXT_DATA__ JSON blob embedded in JobStreet pages.
        This is the most reliable extraction method as it's structured data.
        """
        try:
            data = json.loads(json_string)
            # Navigate the Next.js data structure to find job listings
            # Path may vary; we try common paths
            props = data.get("props", {})
            page_props = props.get("pageProps", {})
            
            # Try different possible keys where jobs might be stored
            jobs_data = (
                page_props.get("jobsResult", {}).get("jobs") or
                page_props.get("jobs") or
                page_props.get("data", {}).get("jobs") or
                []
            )
            
            listings = []
            for job in jobs_data:
                listing = self._extract_listing_from_json(job)
                if listing:
                    listings.append(listing)
            
            return listings
        
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.warning(f"Failed to parse __NEXT_DATA__: {e}")
            return []
    
    def _extract_listing_from_json(self, job: Dict) -> Optional[Dict[str, Any]]:
        """Extracts relevant fields from a single job JSON object."""
        try:
            job_id = str(job.get("id", "") or job.get("jobId", ""))
            if not job_id:
                return None
            
            # Salary extraction
            salary = job.get("salary") or {}
            salary_min = salary.get("minimum") if isinstance(salary, dict) else None
            salary_max = salary.get("maximum") if isinstance(salary, dict) else None
            salary_currency = salary.get("currency") if isinstance(salary, dict) else None
            
            # Company extraction
            company = job.get("advertiser") or job.get("company") or {}
            company_name = (
                company.get("name") if isinstance(company, dict)
                else str(company) if company else "Unknown"
            )
            
            # Location extraction
            location = job.get("location") or {}
            city = (
                location.get("label") or location.get("city")
                if isinstance(location, dict) else str(location)
            )
            
            return {
                "job_id": job_id,
                "title": job.get("title", ""),
                "company_name": company_name,
                "city": city,
                "salary_min": salary_min,
                "salary_max": salary_max,
                "salary_currency": salary_currency,
                "salary_raw": job.get("salaryLabel", ""),
                "posted_date_raw": job.get("listingDate") or job.get("postedDate", ""),
                "url": job.get("jobUrl") or job.get("url", ""),
                "work_arrangement_raw": job.get("workType") or job.get("workArrangement", ""),
                "skills_raw": job.get("skills", []) or [],
                "raw_data": job,  # Preserve complete original
            }
        except Exception as e:
            logger.warning(f"Failed to extract listing from JSON: {e}")
            return None
    
    def _parse_html_listings(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Fallback HTML parser for when __NEXT_DATA__ is unavailable.
        Parses job card elements directly from the page HTML.
        """
        listings = []
        
        # JobStreet job cards — selectors may need adjustment if site structure changes
        job_cards = (
            soup.find_all("div", {"data-automation": "job-card-container"}) or
            soup.find_all("article", class_=re.compile(r"job-card|jobCard", re.I))
        )
        
        for card in job_cards:
            try:
                # Title
                title_el = (
                    card.find("a", {"data-automation": "job-title"}) or
                    card.find("h1", class_=re.compile(r"title", re.I)) or
                    card.find("h2")
                )
                title = title_el.get_text(strip=True) if title_el else ""
                url = title_el.get("href", "") if title_el else ""
                
                # Company
                company_el = card.find("span", {"data-automation": "company-name"})
                company_name = company_el.get_text(strip=True) if company_el else ""
                
                # Location
                location_el = card.find("span", {"data-automation": "job-location"})
                city = location_el.get_text(strip=True) if location_el else ""
                
                # Salary
                salary_el = card.find("span", {"data-automation": "salary-info"})
                salary_raw = salary_el.get_text(strip=True) if salary_el else ""
                
                # Generate a job ID from URL if not found
                job_id = self._extract_job_id_from_url(url) or hashlib.md5(f"{title}{company_name}".encode()).hexdigest()[:12]
                
                if title:
                    listings.append({
                        "job_id": job_id,
                        "title": title,
                        "company_name": company_name,
                        "city": city,
                        "salary_raw": salary_raw,
                        "url": url,
                        "raw_data": {"html_fallback": True},
                    })
            except Exception as e:
                logger.debug(f"Failed to parse job card: {e}")
                continue
        
        return listings
    
    def _scrape_detail_page(self, url: str) -> Dict[str, Any]:
        """
        Fetches and parses a single job posting's detail page.
        Returns additional fields not available on listing pages:
        - Full job description
        - Requirements
        - Experience level
        - Any additional skills
        
        Uses Playwright for JavaScript-rendered content when needed.
        """
        try:
            # First try with requests (faster, less resource-intensive)
            response = self._get(url)
            soup = BeautifulSoup(response.text, "lxml")
            
            # Try __NEXT_DATA__ first (most structured)
            next_data_tag = soup.find("script", {"id": "__NEXT_DATA__"})
            if next_data_tag:
                try:
                    data = json.loads(next_data_tag.string)
                    job_detail = (
                        data.get("props", {})
                        .get("pageProps", {})
                        .get("jobDetail") or
                        data.get("props", {})
                        .get("pageProps", {})
                        .get("job") or {}
                    )
                    if job_detail:
                        return self._extract_detail_from_json(job_detail)
                except Exception:
                    pass
            
            # HTML fallback for detail page
            return self._parse_detail_html(soup)
        
        except Exception as e:
            logger.debug(f"Requests failed for detail page {url}, trying Playwright: {e}")
            return self._scrape_detail_with_playwright(url)
    
    def _scrape_detail_with_playwright(self, url: str) -> Dict[str, Any]:
        """
        Uses Playwright (headless Chromium) to render JavaScript-heavy pages.
        Only used as a fallback when requests-based scraping fails.
        """
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                page.set_extra_http_headers({
                    "User-Agent": self.settings.scraping.user_agent
                })
                
                page.goto(url, timeout=30000, wait_until="domcontentloaded")
                page.wait_for_timeout(2000)  # Allow dynamic content to render
                
                html = page.content()
                browser.close()
            
            soup = BeautifulSoup(html, "lxml")
            return self._parse_detail_html(soup)
        
        except PlaywrightTimeout:
            logger.warning(f"Playwright timeout for URL: {url}")
            return {}
        except Exception as e:
            logger.warning(f"Playwright failed for URL: {url}: {e}")
            return {}
    
    def _extract_detail_from_json(self, job_detail: Dict) -> Dict[str, Any]:
        """Extracts detail fields from JSON-structured job detail data."""
        return {
            "description": job_detail.get("jobDescription", {}).get("text", "") or job_detail.get("description", ""),
            "requirements": job_detail.get("requirements", ""),
            "experience_raw": str(job_detail.get("experience", "") or ""),
            "skills_raw": [
                s.get("label", s) if isinstance(s, dict) else str(s)
                for s in (job_detail.get("skills") or [])
            ],
        }
    
    def _parse_detail_html(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Parses the job detail page HTML to extract description and requirements."""
        result = {}
        
        # Description
        desc_el = (
            soup.find("div", {"data-automation": "jobDescription"}) or
            soup.find("div", class_=re.compile(r"job-description|jobDescription", re.I))
        )
        if desc_el:
            result["description"] = desc_el.get_text(separator="\n", strip=True)
        
        # Requirements (sometimes embedded in description, sometimes separate)
        req_el = soup.find("div", {"data-automation": "jobRequirements"})
        if req_el:
            result["requirements"] = req_el.get_text(separator="\n", strip=True)
        
        return result
    
    def _map_to_raw_posting(
        self,
        item: Dict[str, Any],
        country: CountryCode,
        config: Dict,
    ) -> Optional[RawJobPosting]:
        """
        Maps a scraped item dict to a standardized RawJobPosting.
        This is the final transformation before data leaves the collector.
        """
        title = item.get("title", "").strip()
        company_name = item.get("company_name", "").strip()
        
        # Skip postings with no title or company (data quality gate)
        if not title or not company_name:
            return None
        
        url = item.get("url", "")
        if url and not url.startswith("http"):
            url = config["base_url"] + url
        
        return RawJobPosting(
            source=self.source.value,
            source_job_id=item.get("job_id", ""),
            source_url=url,
            title=title,
            company_name=company_name,
            country=country.value,
            city=item.get("city"),
            description=item.get("description"),
            requirements=item.get("requirements"),
            salary_raw=item.get("salary_raw"),
            posted_date_raw=item.get("posted_date_raw"),
            work_arrangement_raw=item.get("work_arrangement_raw"),
            experience_raw=item.get("experience_raw"),
            skills_raw=item.get("skills_raw", []),
            raw_data=item.get("raw_data", {}),
        )
    
    @staticmethod
    def _extract_job_id_from_url(url: str) -> Optional[str]:
        """Extracts the job ID from a JobStreet URL if present."""
        if not url:
            return None
        # JobStreet URL patterns: /job/{id} or /jobs/{title}-{id}
        match = re.search(r"/job(?:s)?/[\w-]*?(\d{6,})", url)
        return match.group(1) if match else None
