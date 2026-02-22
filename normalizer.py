"""
Normalization Engine — Transforms raw job postings into clean, structured records.

This is the most complex and important processing stage in the pipeline.
Raw job postings come in with inconsistent formats, typos, and missing fields.
The normalizer's job is to produce clean, consistent records that can be:
1. Stored in PostgreSQL with proper types
2. Reliably queried for trend analysis
3. Compared across sources and time periods

Key responsibilities:
- Title normalization → canonical role categories
- Company name normalization → deduplication-ready
- Skill extraction and mapping → canonical skill IDs
- Salary parsing → structured numeric ranges
- Date normalization → consistent date objects
- Experience level classification → enum values
- Work arrangement detection → remote/hybrid/onsite
- Entry-level friendliness scoring
- Deduplication hash generation
"""

import re
import yaml
import hashlib
from datetime import datetime, date
from typing import List, Optional, Tuple, Dict, Set
from rapidfuzz import fuzz, process as rfuzz_process
from dateutil import parser as date_parser

from src.collectors.base import RawJobPosting
from src.storage.models import (
    JobPosting, Company, PostingSkill, Skill,
    CountryCode, WorkArrangement, ExperienceLevel, DataSource
)
from src.storage.database import get_db_session
from src.utils.config import get_settings
from src.utils.logger import get_logger

logger = get_logger(__name__)
settings = get_settings()


# ============================================================
# Normalizer Output
# ============================================================

class NormalizedPosting:
    """Intermediate normalized representation before database insertion."""
    def __init__(self):
        self.title: str = ""
        self.title_normalized: str = ""
        self.company_name: str = ""
        self.company_name_normalized: str = ""
        self.country: Optional[CountryCode] = None
        self.city: Optional[str] = None
        self.description: Optional[str] = None
        self.requirements: Optional[str] = None
        self.work_arrangement: WorkArrangement = WorkArrangement.UNKNOWN
        self.experience_level: ExperienceLevel = ExperienceLevel.UNKNOWN
        self.experience_years_min: Optional[int] = None
        self.experience_years_max: Optional[int] = None
        self.salary_min: Optional[float] = None
        self.salary_max: Optional[float] = None
        self.salary_currency: Optional[str] = None
        self.salary_raw: Optional[str] = None
        self.role_category: Optional[str] = None
        self.is_fresh_graduate_friendly: bool = False
        self.accepts_internship_experience: bool = False
        self.posted_date: Optional[date] = None
        self.skills: List[str] = []       # Canonical skill names
        self.dedup_hash: str = ""
        self.source: str = ""
        self.source_job_id: str = ""
        self.source_url: str = ""
        self.raw_data: dict = {}
        self.data_quality_score: float = 0.0


# ============================================================
# Main Normalizer Class
# ============================================================

class JobPostingNormalizer:
    """
    Normalizes raw job postings from any source into a consistent schema.
    
    Usage:
        normalizer = JobPostingNormalizer()
        normalized = normalizer.normalize(raw_posting)
        # Then pass to StorageHandler for database insertion
    """
    
    def __init__(self, taxonomy_path: str = "config/skill_taxonomy.yaml"):
        self.taxonomy = self._load_taxonomy(taxonomy_path)
        self.skill_aliases = self._build_skill_alias_map()
        self.role_patterns = self._build_role_patterns()
        
        # Cache: skill name → Skill DB record
        self._skill_cache: Dict[str, Optional[int]] = {}
    
    def normalize(self, raw: RawJobPosting) -> Optional[NormalizedPosting]:
        """
        Master normalization method. Applies all normalization steps in sequence.
        Returns None if the posting fails minimum quality checks.
        """
        normalized = NormalizedPosting()
        normalized.source = raw.source
        normalized.source_job_id = raw.source_job_id
        normalized.source_url = raw.source_url
        normalized.raw_data = raw.raw_data or {}
        
        # --- 1. Title normalization ---
        normalized.title = self._clean_text(raw.title)
        normalized.title_normalized = self._normalize_title(normalized.title)
        normalized.role_category = self._classify_role(normalized.title)
        
        # --- 2. Company normalization ---
        normalized.company_name = self._clean_text(raw.company_name)
        normalized.company_name_normalized = self._normalize_company_name(normalized.company_name)
        
        # --- 3. Location ---
        normalized.country = self._parse_country(raw.country)
        normalized.city = self._normalize_city(raw.city)
        
        # --- 4. Description and requirements ---
        normalized.description = self._clean_text(raw.description)
        normalized.requirements = self._clean_text(raw.requirements)
        
        # --- 5. Work arrangement ---
        normalized.work_arrangement = self._classify_work_arrangement(
            raw.work_arrangement_raw, raw.title, raw.description
        )
        
        # --- 6. Experience ---
        normalized.experience_years_min, normalized.experience_years_max = \
            self._parse_experience(raw.experience_raw, raw.description, raw.requirements)
        normalized.experience_level = self._classify_experience_level(
            normalized.experience_years_min, normalized.experience_years_max
        )
        
        # --- 7. Salary ---
        normalized.salary_min, normalized.salary_max, normalized.salary_currency = \
            self._parse_salary(raw.salary_raw, raw.country)
        normalized.salary_raw = raw.salary_raw
        
        # --- 8. Skills extraction ---
        combined_text = " ".join(filter(None, [
            raw.title,
            raw.description,
            raw.requirements,
            " ".join(raw.skills_raw or [])
        ]))
        normalized.skills = self._extract_skills(combined_text)
        
        # --- 9. Entry-level friendliness ---
        all_text = " ".join(filter(None, [raw.title, raw.description, raw.requirements]))
        normalized.is_fresh_graduate_friendly = self._is_fresh_graduate_friendly(
            all_text, normalized.experience_years_min
        )
        normalized.accepts_internship_experience = self._accepts_internship(all_text)
        
        # --- 10. Date parsing ---
        normalized.posted_date = self._parse_date(raw.posted_date_raw)
        
        # --- 11. Deduplication hash ---
        normalized.dedup_hash = self._generate_dedup_hash(normalized)
        
        # --- 12. Quality scoring ---
        normalized.data_quality_score = self._compute_quality_score(normalized)
        
        # --- Quality gate: reject clearly invalid postings ---
        if not normalized.title or not normalized.company_name:
            logger.debug(f"Posting rejected: missing title or company")
            return None
        
        return normalized
    
    def normalize_batch(self, raw_postings: List[RawJobPosting]) -> List[NormalizedPosting]:
        """Normalizes a batch of raw postings. Logs summary statistics."""
        results = []
        failed = 0
        
        for raw in raw_postings:
            try:
                normalized = self.normalize(raw)
                if normalized:
                    results.append(normalized)
                else:
                    failed += 1
            except Exception as e:
                logger.warning(f"Normalization failed for posting '{raw.title}': {e}")
                failed += 1
        
        logger.info(
            f"Normalization complete: {len(results)} succeeded, {failed} rejected/failed "
            f"({len(raw_postings)} total input)"
        )
        return results
    
    # ============================================================
    # Individual Normalization Methods
    # ============================================================
    
    def _clean_text(self, text: Optional[str]) -> Optional[str]:
        """Cleans raw text: strips whitespace, removes null bytes, normalizes newlines."""
        if not text:
            return None
        text = text.replace("\x00", "")          # Remove null bytes
        text = re.sub(r"\r\n", "\n", text)        # Normalize line endings
        text = re.sub(r"[ \t]{2,}", " ", text)    # Collapse multiple spaces/tabs
        text = text.strip()
        return text if text else None
    
    def _normalize_title(self, title: Optional[str]) -> Optional[str]:
        """Normalizes job title to a cleaner, comparable form."""
        if not title:
            return None
        title = title.lower()
        # Remove common noise words that don't affect role classification
        noise_patterns = [
            r"\([\w\s]+\)",        # Anything in parentheses, e.g. "(Contract)"
            r"\[[\w\s]+\]",        # Anything in brackets
            r"\bkl\b",             # Common location abbreviations in titles
            r"\bmy\b|\bsg\b",      # Country codes sometimes in titles
            r"\d+ ?-? ?\d* ?years?",  # Experience years in title (e.g. "5 years")
        ]
        for pattern in noise_patterns:
            title = re.sub(pattern, "", title, flags=re.IGNORECASE)
        return title.strip()
    
    def _normalize_company_name(self, name: Optional[str]) -> Optional[str]:
        """
        Normalizes company names for deduplication.
        "Grab Holdings Inc." and "Grab" should map to the same company.
        """
        if not name:
            return None
        name = name.lower()
        # Remove legal suffixes
        legal_suffixes = [
            r"\bsdn\.?\s*bhd\.?\b",   # Malaysian: Sdn Bhd
            r"\bsdn\b",
            r"\bbhd\b",
            r"\bpte\.?\s*ltd\.?\b",   # Singaporean: Pte Ltd
            r"\bpte\b",
            r"\binc\.?\b",
            r"\bltd\.?\b",
            r"\bcorp\.?\b",
            r"\bco\.?\b",
            r"\bllc\b",
        ]
        for suffix in legal_suffixes:
            name = re.sub(suffix, "", name, flags=re.IGNORECASE)
        
        # Remove special characters, collapse spaces
        name = re.sub(r"[^\w\s]", " ", name)
        name = re.sub(r"\s+", " ", name).strip()
        return name
    
    def _parse_country(self, country_raw: Optional[str]) -> Optional[CountryCode]:
        """Maps raw country string to CountryCode enum."""
        if not country_raw:
            return None
        try:
            return CountryCode(country_raw.upper())
        except ValueError:
            country_map = {
                "malaysia": CountryCode.MALAYSIA,
                "singapore": CountryCode.SINGAPORE,
                "indonesia": CountryCode.INDONESIA,
                "thailand": CountryCode.THAILAND,
                "vietnam": CountryCode.VIETNAM,
                "philippines": CountryCode.PHILIPPINES,
            }
            return country_map.get(country_raw.lower())
    
    def _normalize_city(self, city: Optional[str]) -> Optional[str]:
        """Normalizes city names to canonical forms."""
        if not city:
            return None
        city = city.strip().title()
        # Common normalization mappings
        city_aliases = {
            "Kl": "Kuala Lumpur",
            "Klang Valley": "Kuala Lumpur",
            "Pj": "Petaling Jaya",
            "Cbd": "Central Business District",
        }
        return city_aliases.get(city, city)
    
    def _classify_work_arrangement(
        self,
        raw_type: Optional[str],
        title: Optional[str],
        description: Optional[str],
    ) -> WorkArrangement:
        """Classifies work arrangement from raw fields and text signals."""
        combined = " ".join(filter(None, [raw_type, title, description])).lower()
        
        if any(kw in combined for kw in ["fully remote", "100% remote", "work from home", "wfh", "remote only"]):
            return WorkArrangement.REMOTE
        if any(kw in combined for kw in ["hybrid", "flexible working", "2 days office", "3 days office"]):
            return WorkArrangement.HYBRID
        if any(kw in combined for kw in ["on-site", "onsite", "office based", "in office", "on site"]):
            return WorkArrangement.ONSITE
        if "remote" in combined:
            return WorkArrangement.REMOTE
        
        return WorkArrangement.UNKNOWN
    
    def _parse_experience(
        self,
        experience_raw: Optional[str],
        description: Optional[str],
        requirements: Optional[str],
    ) -> Tuple[Optional[int], Optional[int]]:
        """
        Extracts minimum and maximum years of experience from raw text.
        Handles patterns like: "3-5 years", "minimum 2 years", "3+ years", "at least 5 years"
        """
        combined = " ".join(filter(None, [experience_raw, description, requirements]))
        
        # Pattern: "X to Y years" or "X-Y years"
        range_match = re.search(r"(\d+)\s*(?:to|-)\s*(\d+)\s*years?", combined, re.IGNORECASE)
        if range_match:
            return int(range_match.group(1)), int(range_match.group(2))
        
        # Pattern: "X+ years" or "minimum X years" or "at least X years"
        min_match = re.search(
            r"(?:minimum|min|at least|>)\s*(\d+)\s*years?|(\d+)\+\s*years?",
            combined, re.IGNORECASE
        )
        if min_match:
            years = int(min_match.group(1) or min_match.group(2))
            return years, None
        
        # Pattern: simple "X years"
        simple_match = re.search(r"(\d+)\s*years?\s*(?:of)?\s*experience", combined, re.IGNORECASE)
        if simple_match:
            years = int(simple_match.group(1))
            return years, years
        
        return None, None
    
    def _classify_experience_level(
        self,
        years_min: Optional[int],
        years_max: Optional[int],
    ) -> ExperienceLevel:
        """Maps experience year ranges to standardized experience levels."""
        if years_min is None:
            return ExperienceLevel.UNKNOWN
        if years_min == 0:
            return ExperienceLevel.FRESH
        if years_min <= 2:
            return ExperienceLevel.JUNIOR
        if years_min <= 5:
            return ExperienceLevel.MID
        if years_min <= 8:
            return ExperienceLevel.SENIOR
        return ExperienceLevel.LEAD
    
    def _parse_salary(
        self,
        salary_raw: Optional[str],
        country: Optional[str],
    ) -> Tuple[Optional[float], Optional[float], Optional[str]]:
        """
        Parses salary range from raw string.
        Handles: "MYR 5,000 - 8,000", "RM 5k - 8k", "SGD 6,000/month"
        Returns: (min, max, currency)
        """
        if not salary_raw:
            return None, None, None
        
        # Detect currency
        currency = None
        currency_patterns = {
            "MYR": r"\b(?:myr|rm|ringgit)\b",
            "SGD": r"\b(?:sgd|s\$|singapore dollar)\b",
            "IDR": r"\b(?:idr|rp|rupiah)\b",
        }
        for curr, pattern in currency_patterns.items():
            if re.search(pattern, salary_raw, re.IGNORECASE):
                currency = curr
                break
        
        # Extract numbers (handle k suffix: "5k" = 5000)
        def to_float(s: str) -> Optional[float]:
            s = s.replace(",", "")
            if s.lower().endswith("k"):
                return float(s[:-1]) * 1000
            return float(s) if s else None
        
        # Range pattern: "5,000 - 8,000" or "5k - 8k"
        range_match = re.search(r"([\d,]+k?)\s*[-–to]+\s*([\d,]+k?)", salary_raw, re.IGNORECASE)
        if range_match:
            try:
                sal_min = to_float(range_match.group(1))
                sal_max = to_float(range_match.group(2))
                return sal_min, sal_max, currency
            except (ValueError, TypeError):
                pass
        
        # Single value: "8,000/month"
        single_match = re.search(r"([\d,]+k?)", salary_raw, re.IGNORECASE)
        if single_match:
            try:
                val = to_float(single_match.group(1))
                return val, val, currency
            except (ValueError, TypeError):
                pass
        
        return None, None, currency
    
    def _extract_skills(self, text: str) -> List[str]:
        """
        Extracts canonical skill names from free-form text.
        Uses the skill alias map from taxonomy for matching.
        Returns a deduplicated list of canonical skill names.
        """
        if not text:
            return []
        
        text_lower = text.lower()
        found_skills: Set[str] = set()
        
        for alias, canonical_name in self.skill_aliases.items():
            # Use word boundary matching to avoid false positives
            # e.g., "R" skill shouldn't match "enterprise" or "research"
            if len(alias) <= 2:
                # Short aliases need stricter matching
                pattern = r"\b" + re.escape(alias) + r"\b"
            else:
                pattern = re.escape(alias)
            
            if re.search(pattern, text_lower, re.IGNORECASE):
                found_skills.add(canonical_name)
        
        return list(found_skills)
    
    def _classify_role(self, title: Optional[str]) -> Optional[str]:
        """Maps job title to a canonical role category using the taxonomy patterns."""
        if not title:
            return None
        title_lower = title.lower()
        
        for category, config in self.role_patterns.items():
            for pattern in config:
                if re.search(pattern, title_lower, re.IGNORECASE):
                    return category
        
        return "other"
    
    def _is_fresh_graduate_friendly(
        self,
        text: str,
        experience_years_min: Optional[int],
    ) -> bool:
        """
        Determines if a posting is accessible to fresh graduates.
        A posting is considered fresh-graduate friendly if:
        - Explicitly mentions fresh graduates, OR
        - Requires 0-1 years experience, OR
        - Explicitly states no experience required
        """
        if experience_years_min is not None and experience_years_min <= 1:
            return True
        
        text_lower = text.lower()
        fresh_signals = [
            "fresh graduate", "fresh grad", "new graduate", "recent graduate",
            "no experience required", "no experience needed", "entry level",
            "entry-level", "0 years", "zero experience", "straight from university"
        ]
        return any(signal in text_lower for signal in fresh_signals)
    
    def _accepts_internship(self, text: str) -> bool:
        """Checks if internship experience is accepted in lieu of full-time experience."""
        text_lower = text.lower()
        return any(kw in text_lower for kw in [
            "internship experience", "intern experience", "industrial training",
            "practical experience counts", "internship counts"
        ])
    
    def _parse_date(self, date_raw: Optional[str]) -> Optional[date]:
        """Parses a raw date string into a Python date object."""
        if not date_raw:
            return None
        try:
            return date_parser.parse(date_raw, fuzzy=True).date()
        except (ValueError, OverflowError):
            # Try common relative formats
            date_raw_lower = date_raw.lower()
            today = datetime.utcnow().date()
            
            if "today" in date_raw_lower:
                return today
            
            days_match = re.search(r"(\d+)\s*days?\s*ago", date_raw_lower)
            if days_match:
                from datetime import timedelta
                return today - timedelta(days=int(days_match.group(1)))
            
            return None
    
    def _generate_dedup_hash(self, normalized: NormalizedPosting) -> str:
        """
        Generates a deduplication hash for a normalized posting.
        Same company + same normalized title + same country = duplicate.
        """
        desc_snippet = (normalized.description or "")[:100].lower().strip()
        key = f"{normalized.company_name_normalized}|{normalized.title_normalized}|{normalized.country}"
        if desc_snippet:
            key += f"|{desc_snippet}"
        return hashlib.sha256(key.encode()).hexdigest()
    
    def _compute_quality_score(self, normalized: NormalizedPosting) -> float:
        """
        Scores data completeness on a 0.0 to 1.0 scale.
        Used to flag low-quality records and prioritize high-quality ones.
        """
        score = 0.0
        weights = {
            "title": 0.15,
            "company": 0.15,
            "description": 0.20,
            "country": 0.10,
            "city": 0.05,
            "skills": 0.15,
            "experience": 0.10,
            "salary": 0.05,
            "date": 0.05,
        }
        
        if normalized.title:
            score += weights["title"]
        if normalized.company_name:
            score += weights["company"]
        if normalized.description and len(normalized.description) > 100:
            score += weights["description"]
        if normalized.country:
            score += weights["country"]
        if normalized.city:
            score += weights["city"]
        if normalized.skills:
            score += weights["skills"]
        if normalized.experience_years_min is not None:
            score += weights["experience"]
        if normalized.salary_min is not None:
            score += weights["salary"]
        if normalized.posted_date:
            score += weights["date"]
        
        return round(score, 2)
    
    # ============================================================
    # Taxonomy Loading Helpers
    # ============================================================
    
    def _load_taxonomy(self, path: str) -> Dict:
        """Loads the skill taxonomy YAML file."""
        try:
            with open(path, "r") as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning(f"Taxonomy file not found: {path}")
            return {"skills": {}, "role_categories": {}}
    
    def _build_skill_alias_map(self) -> Dict[str, str]:
        """
        Builds a flat mapping of alias → canonical_name from the taxonomy.
        This is what's used for fast skill extraction in _extract_skills().
        
        Example output: {"python3": "python", "sklearn": "scikit_learn", ...}
        """
        alias_map = {}
        skills_config = self.taxonomy.get("skills", {})
        
        for canonical_name, config in skills_config.items():
            # The canonical name itself is also a valid alias
            alias_map[canonical_name] = canonical_name
            alias_map[canonical_name.replace("_", " ")] = canonical_name
            
            # All defined aliases
            for alias in config.get("aliases", []):
                alias_map[alias.lower()] = canonical_name
        
        return alias_map
    
    def _build_role_patterns(self) -> Dict[str, List[str]]:
        """Builds regex patterns for role classification from taxonomy."""
        role_categories = self.taxonomy.get("role_categories", {})
        patterns = {}
        
        for category, config in role_categories.items():
            patterns[category] = [
                re.escape(p).replace(r"\ ", r"\s+")
                for p in config.get("patterns", [])
            ]
        
        return patterns
