"""
Database Models — SQLAlchemy ORM definitions.

These models define the schema for our PostgreSQL database.
Every table is documented with its purpose so the schema tells a clear story.

Schema Design Principles:
- Raw postings are stored as-is (job_postings) — never mutate source data
- Processed/normalized data lives in separate snapshot tables
- Junction tables handle many-to-many relationships cleanly
- All tables have created_at/updated_at for audit trails
"""

from datetime import datetime
from typing import Optional, List
from sqlalchemy import (
    Column, Integer, String, Text, Float, Boolean,
    DateTime, Date, ForeignKey, UniqueConstraint,
    Index, Enum as SAEnum
)
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from sqlalchemy.orm import relationship, DeclarativeBase
import enum


class Base(DeclarativeBase):
    pass


# ============================================================
# Enums
# ============================================================

class CountryCode(str, enum.Enum):
    MALAYSIA = "MY"
    SINGAPORE = "SG"
    INDONESIA = "ID"
    THAILAND = "TH"
    VIETNAM = "VN"
    PHILIPPINES = "PH"


class WorkArrangement(str, enum.Enum):
    REMOTE = "remote"
    HYBRID = "hybrid"
    ONSITE = "onsite"
    UNKNOWN = "unknown"


class ExperienceLevel(str, enum.Enum):
    FRESH = "fresh_graduate"      # 0 years
    JUNIOR = "junior"             # 1-2 years
    MID = "mid"                   # 3-5 years
    SENIOR = "senior"             # 5+ years
    LEAD = "lead"                 # Lead/Principal/Staff
    UNKNOWN = "unknown"


class DataSource(str, enum.Enum):
    JOBSTREET = "jobstreet"
    LINKEDIN = "linkedin"
    TECHINASIA = "techinasia"
    INDEED = "indeed"
    GLINTS = "glints"
    COMPANY_CAREER = "company_career_page"


# ============================================================
# Core Tables
# ============================================================

class Company(Base):
    """
    Normalized company profiles.
    One row per company, regardless of how many job postings they have.
    Company names are normalized (deduplicated) during ingestion.
    """
    __tablename__ = "companies"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    name_normalized = Column(String(255), nullable=False)  # lowercase, stripped
    country = Column(SAEnum(CountryCode), nullable=True)
    city = Column(String(100), nullable=True)
    industry = Column(String(100), nullable=True)
    company_size = Column(String(50), nullable=True)       # e.g. "51-200", "1000+"
    website = Column(String(500), nullable=True)
    linkedin_url = Column(String(500), nullable=True)
    is_tech_company = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    job_postings = relationship("JobPosting", back_populates="company")
    snapshots = relationship("CompanySnapshot", back_populates="company")

    __table_args__ = (
        UniqueConstraint("name_normalized", name="uq_company_name_normalized"),
        Index("idx_company_country", "country"),
    )

    def __repr__(self):
        return f"<Company(id={self.id}, name={self.name}, country={self.country})>"


class Skill(Base):
    """
    Master skill registry — canonical skill names.
    All skill mentions in job postings are mapped to entries here
    via the skill_taxonomy.yaml config. This ensures "Python", "python3",
    and "Python Programming" all count as the same skill.
    """
    __tablename__ = "skills"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True)       # e.g. "python"
    display_name = Column(String(100), nullable=False)            # e.g. "Python"
    category = Column(String(50), nullable=True)                  # e.g. "programming_language"
    subcategory = Column(String(50), nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    posting_skills = relationship("PostingSkill", back_populates="skill")
    snapshots = relationship("SkillSnapshot", back_populates="skill")

    __table_args__ = (
        Index("idx_skill_category", "category"),
    )

    def __repr__(self):
        return f"<Skill(id={self.id}, name={self.name}, category={self.category})>"


class JobPosting(Base):
    """
    Core table — one row per unique job posting.
    
    Raw data is preserved in raw_data (JSONB) so we never lose original content.
    Normalized fields are extracted and stored separately for querying efficiency.
    
    Deduplication logic ensures the same job posted on multiple platforms
    is stored once, with the source_urls field tracking all platforms.
    """
    __tablename__ = "job_postings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # --- Source tracking ---
    source = Column(SAEnum(DataSource), nullable=False)
    source_job_id = Column(String(255), nullable=True)   # The ID from the source platform
    source_url = Column(String(1000), nullable=True)
    source_urls = Column(ARRAY(String), default=list)    # All URLs if same job found on multiple sources
    
    # --- Core fields ---
    title = Column(String(500), nullable=False)
    title_normalized = Column(String(500), nullable=True)  # Mapped to canonical role category
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=True)
    country = Column(SAEnum(CountryCode), nullable=True)
    city = Column(String(100), nullable=True)
    
    # --- Job details ---
    description = Column(Text, nullable=True)
    requirements = Column(Text, nullable=True)
    work_arrangement = Column(SAEnum(WorkArrangement), default=WorkArrangement.UNKNOWN)
    experience_level = Column(SAEnum(ExperienceLevel), default=ExperienceLevel.UNKNOWN)
    experience_years_min = Column(Integer, nullable=True)
    experience_years_max = Column(Integer, nullable=True)
    
    # --- Compensation ---
    salary_min = Column(Float, nullable=True)         # In local currency
    salary_max = Column(Float, nullable=True)
    salary_currency = Column(String(10), nullable=True)
    salary_raw = Column(String(200), nullable=True)   # Original salary string
    
    # --- Classification ---
    role_category = Column(String(100), nullable=True)   # Mapped via taxonomy
    is_fresh_graduate_friendly = Column(Boolean, default=False)
    accepts_internship_experience = Column(Boolean, default=False)
    
    # --- Temporal ---
    posted_date = Column(Date, nullable=True)
    collected_at = Column(DateTime, default=datetime.utcnow)
    
    # --- Quality ---
    raw_data = Column(JSONB, nullable=True)           # Original scraped data preserved
    data_quality_score = Column(Float, nullable=True)  # 0.0-1.0, set by Great Expectations
    is_duplicate = Column(Boolean, default=False)
    duplicate_of_id = Column(Integer, ForeignKey("job_postings.id"), nullable=True)
    
    # Relationships
    company = relationship("Company", back_populates="job_postings")
    posting_skills = relationship("PostingSkill", back_populates="posting", cascade="all, delete-orphan")

    __table_args__ = (
        Index("idx_posting_country_date", "country", "posted_date"),
        Index("idx_posting_role_category", "role_category"),
        Index("idx_posting_company", "company_id"),
        Index("idx_posting_source", "source"),
        Index("idx_posting_collected_at", "collected_at"),
    )

    def __repr__(self):
        return f"<JobPosting(id={self.id}, title={self.title[:30]}, country={self.country})>"


class PostingSkill(Base):
    """
    Junction table — many job postings reference many skills.
    Each row represents one skill extracted from one job posting.
    """
    __tablename__ = "posting_skills"

    id = Column(Integer, primary_key=True, autoincrement=True)
    posting_id = Column(Integer, ForeignKey("job_postings.id", ondelete="CASCADE"), nullable=False)
    skill_id = Column(Integer, ForeignKey("skills.id"), nullable=False)
    is_required = Column(Boolean, default=True)     # Required vs preferred/nice-to-have
    raw_mention = Column(String(200), nullable=True) # Original text before normalization

    # Relationships
    posting = relationship("JobPosting", back_populates="posting_skills")
    skill = relationship("Skill", back_populates="posting_skills")

    __table_args__ = (
        UniqueConstraint("posting_id", "skill_id", name="uq_posting_skill"),
        Index("idx_posting_skill_skill_id", "skill_id"),
    )


# ============================================================
# Snapshot Tables (Aggregated Analytics)
# These are pre-aggregated for fast dashboard queries.
# They're populated by scheduled Airflow DAGs.
# ============================================================

class SkillSnapshot(Base):
    """
    Weekly aggregated skill demand counts.
    Instead of counting live every time the dashboard loads,
    we pre-aggregate these weekly for fast querying.
    """
    __tablename__ = "skill_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    skill_id = Column(Integer, ForeignKey("skills.id"), nullable=False)
    snapshot_week = Column(Date, nullable=False)     # Monday of the week
    country = Column(SAEnum(CountryCode), nullable=True)  # NULL = all countries combined
    
    # Demand metrics
    posting_count = Column(Integer, default=0)       # Postings mentioning this skill
    total_postings = Column(Integer, default=0)      # Total postings in period (for % calc)
    demand_pct = Column(Float, nullable=True)        # posting_count / total_postings
    
    # Week-over-week change
    prev_week_count = Column(Integer, nullable=True)
    wow_change_pct = Column(Float, nullable=True)    # Week-over-week % change

    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    skill = relationship("Skill", back_populates="snapshots")

    __table_args__ = (
        UniqueConstraint("skill_id", "snapshot_week", "country", name="uq_skill_snapshot"),
        Index("idx_skill_snapshot_week", "snapshot_week"),
        Index("idx_skill_snapshot_country", "country"),
    )


class CompanySnapshot(Base):
    """
    Weekly aggregated company hiring activity.
    Used for the Company Hiring Velocity Index.
    """
    __tablename__ = "company_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    snapshot_week = Column(Date, nullable=False)
    
    # Volume metrics
    new_postings = Column(Integer, default=0)
    active_postings = Column(Integer, default=0)
    
    # Role breakdown (JSON for flexibility)
    role_breakdown = Column(JSONB, nullable=True)    # {"data_scientist": 3, "ml_engineer": 1}
    
    # Velocity
    prev_week_postings = Column(Integer, nullable=True)
    wow_velocity = Column(Float, nullable=True)      # % change week-over-week

    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    company = relationship("Company", back_populates="snapshots")

    __table_args__ = (
        UniqueConstraint("company_id", "snapshot_week", name="uq_company_snapshot"),
        Index("idx_company_snapshot_week", "snapshot_week"),
    )


class MarketEvent(Base):
    """
    External market events correlated with hiring trends.
    Used for the Market Sentiment Pulse feature (V2).
    Examples: funding announcements, regulatory changes, layoffs.
    """
    __tablename__ = "market_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    event_date = Column(Date, nullable=False)
    event_type = Column(String(50), nullable=False)  # "funding", "layoff", "regulation", "product_launch"
    title = Column(String(500), nullable=False)
    description = Column(Text, nullable=True)
    source_url = Column(String(1000), nullable=True)
    affected_country = Column(SAEnum(CountryCode), nullable=True)
    affected_companies = Column(ARRAY(String), default=list)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_market_event_date", "event_date"),
        Index("idx_market_event_type", "event_type"),
    )


class PipelineRun(Base):
    """
    Audit log for every pipeline execution.
    Tracks what ran, when, how many records were processed, and whether it succeeded.
    Essential for debugging and monitoring the health of the data collection layer.
    """
    __tablename__ = "pipeline_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    dag_id = Column(String(200), nullable=False)
    run_id = Column(String(200), nullable=False)
    source = Column(SAEnum(DataSource), nullable=True)
    country = Column(SAEnum(CountryCode), nullable=True)
    
    started_at = Column(DateTime, nullable=False)
    completed_at = Column(DateTime, nullable=True)
    status = Column(String(50), nullable=False, default="running")  # running, success, failed, partial
    
    records_collected = Column(Integer, default=0)
    records_normalized = Column(Integer, default=0)
    records_deduplicated = Column(Integer, default=0)
    records_stored = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)
    
    error_message = Column(Text, nullable=True)
    metadata = Column(JSONB, nullable=True)  # Flexible field for source-specific metrics

    __table_args__ = (
        Index("idx_pipeline_run_dag", "dag_id"),
        Index("idx_pipeline_run_status", "status"),
        Index("idx_pipeline_run_started", "started_at"),
    )

    def __repr__(self):
        return f"<PipelineRun(id={self.id}, dag={self.dag_id}, status={self.status})>"
