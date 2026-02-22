"""
Unit tests for the JobPostingNormalizer.

Tests cover all critical normalization methods individually.
These tests don't require a database connection — they test pure logic.

Run with: pytest tests/unit/test_normalizer.py -v
"""

import pytest
from datetime import date
from unittest.mock import patch, MagicMock

from src.collectors.base import RawJobPosting
from src.normalizers.normalizer import JobPostingNormalizer
from src.storage.models import WorkArrangement, ExperienceLevel


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def normalizer():
    """Returns a normalizer instance with a mocked taxonomy."""
    with patch.object(JobPostingNormalizer, "_load_taxonomy") as mock_tax:
        mock_tax.return_value = {
            "skills": {
                "python": {"aliases": ["python3", "python programming"], "category": "programming_language"},
                "sql": {"aliases": ["mysql", "postgresql"], "category": "programming_language"},
                "machine_learning": {"aliases": ["ml", "machine learning"], "category": "mathematics"},
                "docker": {"aliases": ["dockerfile"], "category": "mlops"},
            },
            "role_categories": {
                "data_scientist": {"patterns": ["data scientist", "data science"]},
                "ml_engineer": {"patterns": ["machine learning engineer", "ml engineer"]},
                "data_engineer": {"patterns": ["data engineer"]},
            }
        }
        return JobPostingNormalizer()


@pytest.fixture
def sample_raw_posting():
    """A realistic raw posting for testing."""
    return RawJobPosting(
        source="jobstreet",
        source_job_id="123456",
        source_url="https://www.jobstreet.com.my/job/123456",
        title="Senior Data Scientist",
        company_name="Grab Holdings Sdn Bhd",
        country="MY",
        city="Kuala Lumpur",
        description=(
            "We are looking for an experienced Data Scientist to join our team. "
            "You will work with Python, SQL, and machine learning frameworks to build "
            "predictive models. Experience with Docker and cloud platforms is a plus. "
            "3-5 years of experience required. Hybrid working arrangement available."
        ),
        requirements=(
            "- Proficiency in Python, SQL\n"
            "- Experience with machine learning\n"
            "- 3-5 years experience\n"
            "- Docker knowledge preferred"
        ),
        salary_raw="MYR 8,000 - 12,000",
        posted_date_raw="2025-02-20",
        work_arrangement_raw="Hybrid",
        experience_raw="3-5 years",
        skills_raw=["Python", "SQL", "Machine Learning"],
        raw_data={"original": True},
    )


# ============================================================
# Tests: Company Name Normalization
# ============================================================

class TestCompanyNormalization:
    
    def test_removes_sdn_bhd(self, normalizer):
        assert normalizer._normalize_company_name("Grab Holdings Sdn Bhd") == "grab holdings"
    
    def test_removes_pte_ltd(self, normalizer):
        assert normalizer._normalize_company_name("Sea Limited Pte Ltd") == "sea limited"
    
    def test_removes_inc(self, normalizer):
        assert normalizer._normalize_company_name("AirAsia Inc.") == "airasia"
    
    def test_handles_none(self, normalizer):
        assert normalizer._normalize_company_name(None) is None
    
    def test_handles_empty_string(self, normalizer):
        assert normalizer._normalize_company_name("") is None
    
    def test_collapses_extra_spaces(self, normalizer):
        result = normalizer._normalize_company_name("Shopee   Pte   Ltd")
        assert "  " not in result


# ============================================================
# Tests: Salary Parsing
# ============================================================

class TestSalaryParsing:
    
    def test_parses_myr_range(self, normalizer):
        min_sal, max_sal, currency = normalizer._parse_salary("MYR 5,000 - 8,000", "MY")
        assert min_sal == 5000.0
        assert max_sal == 8000.0
        assert currency == "MYR"
    
    def test_parses_rm_format(self, normalizer):
        min_sal, max_sal, currency = normalizer._parse_salary("RM 5,000 - RM 8,000", "MY")
        assert min_sal == 5000.0
        assert max_sal == 8000.0
    
    def test_parses_k_notation(self, normalizer):
        min_sal, max_sal, currency = normalizer._parse_salary("MYR 5k - 8k", "MY")
        assert min_sal == 5000.0
        assert max_sal == 8000.0
    
    def test_parses_sgd(self, normalizer):
        min_sal, max_sal, currency = normalizer._parse_salary("SGD 6,000 - 9,000", "SG")
        assert currency == "SGD"
        assert min_sal == 6000.0
    
    def test_handles_none(self, normalizer):
        min_sal, max_sal, currency = normalizer._parse_salary(None, "MY")
        assert min_sal is None
        assert max_sal is None
        assert currency is None
    
    def test_handles_no_salary_text(self, normalizer):
        min_sal, max_sal, currency = normalizer._parse_salary("Competitive salary", "MY")
        # Should return None gracefully — not crash
        assert min_sal is None


# ============================================================
# Tests: Experience Parsing
# ============================================================

class TestExperienceParsing:
    
    def test_parses_range(self, normalizer):
        min_y, max_y = normalizer._parse_experience("3-5 years", None, None)
        assert min_y == 3
        assert max_y == 5
    
    def test_parses_minimum(self, normalizer):
        min_y, max_y = normalizer._parse_experience("minimum 3 years", None, None)
        assert min_y == 3
    
    def test_parses_plus_notation(self, normalizer):
        min_y, max_y = normalizer._parse_experience("5+ years experience", None, None)
        assert min_y == 5
    
    def test_parses_from_description(self, normalizer):
        desc = "We need someone with at least 2 years of experience in data science."
        min_y, max_y = normalizer._parse_experience(None, desc, None)
        assert min_y == 2
    
    def test_handles_none(self, normalizer):
        min_y, max_y = normalizer._parse_experience(None, None, None)
        assert min_y is None
        assert max_y is None


# ============================================================
# Tests: Work Arrangement Classification
# ============================================================

class TestWorkArrangementClassification:
    
    def test_detects_hybrid(self, normalizer):
        result = normalizer._classify_work_arrangement("Hybrid", None, None)
        assert result == WorkArrangement.HYBRID
    
    def test_detects_remote_in_description(self, normalizer):
        result = normalizer._classify_work_arrangement(None, None, "This is a fully remote position.")
        assert result == WorkArrangement.REMOTE
    
    def test_detects_wfh(self, normalizer):
        result = normalizer._classify_work_arrangement(None, "WFH Data Analyst", None)
        assert result == WorkArrangement.REMOTE
    
    def test_detects_onsite(self, normalizer):
        result = normalizer._classify_work_arrangement("on-site", None, None)
        assert result == WorkArrangement.ONSITE
    
    def test_unknown_fallback(self, normalizer):
        result = normalizer._classify_work_arrangement(None, "Data Scientist", "Build ML models.")
        assert result == WorkArrangement.UNKNOWN


# ============================================================
# Tests: Skill Extraction
# ============================================================

class TestSkillExtraction:
    
    def test_extracts_python(self, normalizer):
        skills = normalizer._extract_skills("Experience with Python programming required.")
        assert "python" in skills
    
    def test_extracts_alias(self, normalizer):
        # "python3" should map to "python"
        skills = normalizer._extract_skills("Must know Python3 and MySQL.")
        assert "python" in skills
        assert "sql" in skills
    
    def test_extracts_multiple_skills(self, normalizer):
        skills = normalizer._extract_skills("Python, SQL, and Machine Learning experience needed.")
        assert "python" in skills
        assert "sql" in skills
        assert "machine_learning" in skills
    
    def test_deduplicates_skills(self, normalizer):
        # "python" and "python3" should not produce two "python" entries
        skills = normalizer._extract_skills("Python and python3 experience.")
        assert skills.count("python") == 1
    
    def test_handles_empty_text(self, normalizer):
        skills = normalizer._extract_skills("")
        assert skills == []


# ============================================================
# Tests: Fresh Graduate Detection
# ============================================================

class TestFreshGraduateDetection:
    
    def test_detects_explicit_mention(self, normalizer):
        text = "Fresh graduates are welcome to apply."
        assert normalizer._is_fresh_graduate_friendly(text, None) is True
    
    def test_detects_zero_experience(self, normalizer):
        assert normalizer._is_fresh_graduate_friendly("Some role", 0) is True
    
    def test_detects_entry_level(self, normalizer):
        assert normalizer._is_fresh_graduate_friendly("Entry-level position available.", None) is True
    
    def test_rejects_senior_role(self, normalizer):
        text = "Senior position requiring deep expertise."
        assert normalizer._is_fresh_graduate_friendly(text, 5) is False


# ============================================================
# Tests: Full Normalization Pipeline
# ============================================================

class TestFullNormalization:
    
    def test_normalizes_complete_posting(self, normalizer, sample_raw_posting):
        result = normalizer.normalize(sample_raw_posting)
        
        assert result is not None
        assert result.title == "Senior Data Scientist"
        assert result.company_name_normalized == "grab holdings"
        assert result.salary_min == 8000.0
        assert result.salary_max == 12000.0
        assert result.salary_currency == "MYR"
        assert result.experience_years_min == 3
        assert result.experience_years_max == 5
        assert result.work_arrangement == WorkArrangement.HYBRID
        assert "python" in result.skills
        assert result.posted_date == date(2025, 2, 20)
    
    def test_rejects_posting_without_title(self, normalizer):
        posting = RawJobPosting(
            source="jobstreet",
            source_job_id="999",
            source_url="https://example.com",
            title="",
            company_name="Some Company",
            country="MY",
        )
        result = normalizer.normalize(posting)
        assert result is None
    
    def test_quality_score_complete_posting(self, normalizer, sample_raw_posting):
        result = normalizer.normalize(sample_raw_posting)
        assert result is not None
        assert result.data_quality_score > 0.7  # Well-filled posting should score high
    
    def test_quality_score_sparse_posting(self, normalizer):
        posting = RawJobPosting(
            source="jobstreet",
            source_job_id="100",
            source_url="https://example.com",
            title="Data Analyst",
            company_name="Some Company",
            country="MY",
        )
        result = normalizer.normalize(posting)
        assert result is not None
        assert result.data_quality_score < 0.5  # Sparse posting should score low
    
    def test_dedup_hash_consistency(self, normalizer, sample_raw_posting):
        result1 = normalizer.normalize(sample_raw_posting)
        result2 = normalizer.normalize(sample_raw_posting)
        assert result1.dedup_hash == result2.dedup_hash
    
    def test_batch_normalization(self, normalizer, sample_raw_posting):
        postings = [sample_raw_posting] * 3
        results = normalizer.normalize_batch(postings)
        assert len(results) == 3
