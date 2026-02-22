"""
Main Collection DAG — Orchestrates the full data collection pipeline.

This DAG runs daily and coordinates:
1. Data collection from all sources (JobStreet MY, JobStreet SG, ...)
2. Normalization of raw postings
3. Deduplication against existing records
4. Storage to PostgreSQL
5. Pipeline run logging (success/failure metrics)
6. (Future) Trigger downstream ML retraining if enough new data

DAG Design Principles:
- Each source × country runs as an independent task group
  so a failure in one source doesn't kill other collection tasks
- Tasks are idempotent — safe to re-run if a previous run failed midway
- Full audit trail via the pipeline_runs table
- Failure alerting via task failure callbacks

Schedule: Daily at 2:00 AM MYT (UTC+8) → 6:00 PM UTC
"""

from datetime import datetime, timedelta
from typing import Dict, Any, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

# Import our application modules
# These are available because our src/ directory is mounted into the Airflow container
import sys
sys.path.insert(0, "/opt/airflow")

from src.collectors.jobstreet import JobStreetCollector
from src.normalizers.normalizer import JobPostingNormalizer
from src.storage.database import get_db_session, init_database
from src.storage.models import (
    JobPosting, Company, PostingSkill, Skill,
    PipelineRun, CountryCode, DataSource
)
from src.utils.logger import get_logger

logger = get_logger(__name__)


# ============================================================
# DAG Default Arguments
# ============================================================

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,      # Set to True with SMTP config for alerts
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=4),
}


# ============================================================
# Task Functions
# ============================================================

def task_initialize_db(**context) -> None:
    """
    Ensures database tables and MinIO buckets exist.
    This is idempotent — safe to run every DAG execution.
    """
    from src.storage.database import init_database, init_minio_buckets
    logger.info("Initializing database and storage...")
    init_database()
    init_minio_buckets()
    logger.info("Initialization complete.")


def task_collect_source(
    source_name: str,
    country_code: str,
    **context
) -> Dict[str, Any]:
    """
    Airflow task: Runs a specific collector for a specific country.
    
    Returns a result dict that gets pushed to XCom so downstream tasks
    (normalization, storage) can access the collected data and metadata.
    
    XCom key: f"collect_{source_name}_{country_code}"
    """
    country = CountryCode(country_code)
    
    # Instantiate the appropriate collector
    collector_map = {
        "jobstreet": JobStreetCollector,
        # Future: "linkedin": LinkedInCollector,
        # Future: "techinasia": TechInAsiaCollector,
    }
    
    collector_class = collector_map.get(source_name)
    if not collector_class:
        raise ValueError(f"Unknown collector: {source_name}")
    
    collector = collector_class()
    result = collector.run(country=country)
    
    logger.info(
        f"Collection task complete: {source_name}/{country_code} "
        f"→ {result['records_collected']} records, status={result['status']}"
    )
    
    # Log pipeline run to database
    _log_pipeline_run(
        dag_id=context["dag"].dag_id,
        run_id=context["run_id"],
        source=source_name,
        country=country_code,
        records_collected=result["records_collected"],
        status=result["status"],
        error=result.get("error"),
    )
    
    # Push result to XCom (postings list serialized to dicts)
    postings_as_dicts = [p.to_dict() for p in result.get("postings", [])]
    context["ti"].xcom_push(
        key=f"collect_{source_name}_{country_code}",
        value={
            "status": result["status"],
            "records_collected": result["records_collected"],
            "raw_path": result.get("raw_path"),
            "batch_id": result["batch_id"],
            "postings": postings_as_dicts,
        }
    )
    
    return result["records_collected"]


def task_normalize_and_store(
    source_name: str,
    country_code: str,
    **context
) -> Dict[str, Any]:
    """
    Airflow task: Normalizes and stores postings collected by the upstream task.
    
    Pulls raw postings from XCom, runs them through the normalizer,
    performs deduplication, and inserts clean records into PostgreSQL.
    """
    from src.collectors.base import RawJobPosting
    
    # Pull collected data from XCom
    ti = context["ti"]
    collect_result = ti.xcom_pull(
        key=f"collect_{source_name}_{country_code}",
        task_ids=f"collect_{source_name}_{country_code}"
    )
    
    if not collect_result or collect_result.get("status") == "failed":
        logger.warning(f"No valid data to normalize for {source_name}/{country_code}")
        return {"status": "skipped", "records_stored": 0}
    
    raw_dicts = collect_result.get("postings", [])
    if not raw_dicts:
        logger.info(f"Empty postings list for {source_name}/{country_code}")
        return {"status": "success", "records_stored": 0}
    
    # Reconstruct RawJobPosting objects from XCom dicts
    raw_postings = [RawJobPosting(**d) for d in raw_dicts]
    
    # Normalize
    normalizer = JobPostingNormalizer()
    normalized_postings = normalizer.normalize_batch(raw_postings)
    
    # Store to database
    stored_count, dedup_count = _store_normalized_postings(normalized_postings)
    
    logger.info(
        f"Storage complete: {source_name}/{country_code} → "
        f"{stored_count} stored, {dedup_count} deduplicated"
    )
    
    return {
        "status": "success",
        "records_normalized": len(normalized_postings),
        "records_stored": stored_count,
        "records_deduplicated": dedup_count,
    }


def task_compute_weekly_snapshots(**context) -> None:
    """
    Airflow task: Computes and updates the skill and company snapshot tables.
    These pre-aggregated tables power fast dashboard queries.
    Runs after all collection and storage tasks complete.
    """
    from src.analytics.snapshots import SnapshotComputer
    
    snapshot_computer = SnapshotComputer()
    snapshot_computer.compute_skill_snapshots()
    snapshot_computer.compute_company_snapshots()
    
    logger.info("Weekly snapshots computed and stored.")


def task_pipeline_health_check(**context) -> None:
    """
    Final task: Validates the pipeline run was healthy.
    Logs summary metrics and flags any anomalies.
    """
    # Pull all normalize/store results from XCom
    ti = context["ti"]
    total_stored = 0
    
    for source in ["jobstreet"]:
        for country in ["MY", "SG"]:
            result = ti.xcom_pull(
                key=f"normalize_{source}_{country}",
                task_ids=f"normalize_{source}_{country}"
            )
            if result:
                total_stored += result.get("records_stored", 0)
    
    logger.info(f"Pipeline health check: total records stored this run = {total_stored}")
    
    if total_stored == 0:
        logger.warning("ALERT: Zero records stored in this pipeline run. Check collector logs.")
    
    # Future: Send Slack notification with summary metrics


# ============================================================
# Helper Functions
# ============================================================

def _store_normalized_postings(normalized_postings) -> tuple:
    """
    Stores normalized postings to PostgreSQL.
    Handles: company upsert, deduplication check, posting insert, skill linking.
    Returns: (stored_count, deduplicated_count)
    """
    stored = 0
    deduped = 0
    
    with get_db_session() as session:
        # Load existing dedup hashes for fast lookup
        existing_hashes = {
            row[0] for row in
            session.query(JobPosting.dedup_hash if hasattr(JobPosting, 'dedup_hash') else JobPosting.id).all()
        }
        
        # Load skill name → id mapping
        skill_map = {
            skill.name: skill.id
            for skill in session.query(Skill).all()
        }
        
        for norm in normalized_postings:
            try:
                # Deduplication check
                if norm.dedup_hash in existing_hashes:
                    deduped += 1
                    continue
                existing_hashes.add(norm.dedup_hash)
                
                # Upsert company
                company_id = _upsert_company(session, norm)
                
                # Create job posting
                posting = JobPosting(
                    source=DataSource(norm.source),
                    source_job_id=norm.source_job_id,
                    source_url=norm.source_url,
                    title=norm.title,
                    title_normalized=norm.title_normalized,
                    company_id=company_id,
                    country=norm.country,
                    city=norm.city,
                    description=norm.description,
                    requirements=norm.requirements,
                    work_arrangement=norm.work_arrangement,
                    experience_level=norm.experience_level,
                    experience_years_min=norm.experience_years_min,
                    experience_years_max=norm.experience_years_max,
                    salary_min=norm.salary_min,
                    salary_max=norm.salary_max,
                    salary_currency=norm.salary_currency,
                    salary_raw=norm.salary_raw,
                    role_category=norm.role_category,
                    is_fresh_graduate_friendly=norm.is_fresh_graduate_friendly,
                    accepts_internship_experience=norm.accepts_internship_experience,
                    posted_date=norm.posted_date,
                    raw_data=norm.raw_data,
                    data_quality_score=norm.data_quality_score,
                )
                session.add(posting)
                session.flush()  # Get the posting ID without committing
                
                # Link skills
                for skill_name in norm.skills:
                    skill_id = skill_map.get(skill_name)
                    if skill_id:
                        posting_skill = PostingSkill(
                            posting_id=posting.id,
                            skill_id=skill_id,
                            is_required=True,
                        )
                        session.add(posting_skill)
                
                stored += 1
                
            except Exception as e:
                logger.warning(f"Failed to store posting '{norm.title}': {e}")
                continue
    
    return stored, deduped


def _upsert_company(session, norm) -> int:
    """
    Finds or creates a company record.
    Matches on normalized company name to avoid duplicates.
    Returns the company ID.
    """
    existing = session.query(Company).filter(
        Company.name_normalized == norm.company_name_normalized
    ).first()
    
    if existing:
        return existing.id
    
    company = Company(
        name=norm.company_name,
        name_normalized=norm.company_name_normalized,
        country=norm.country,
        city=norm.city,
    )
    session.add(company)
    session.flush()
    return company.id


def _log_pipeline_run(
    dag_id: str,
    run_id: str,
    source: str,
    country: str,
    records_collected: int,
    status: str,
    error: str = None,
) -> None:
    """Logs a pipeline run record to the database for audit/monitoring."""
    try:
        with get_db_session() as session:
            run = PipelineRun(
                dag_id=dag_id,
                run_id=run_id,
                source=DataSource(source) if source in [e.value for e in DataSource] else None,
                country=CountryCode(country) if country in [e.value for e in CountryCode] else None,
                started_at=datetime.utcnow(),
                completed_at=datetime.utcnow(),
                status=status,
                records_collected=records_collected,
                error_message=error,
            )
            session.add(run)
    except Exception as e:
        logger.warning(f"Failed to log pipeline run: {e}")


# ============================================================
# DAG Definition
# ============================================================

with DAG(
    dag_id="job_market_collection_daily",
    description="Daily job market data collection from SEA job boards",
    default_args=default_args,
    schedule_interval="0 18 * * *",    # 6:00 PM UTC = 2:00 AM MYT
    catchup=False,                      # Don't backfill missed runs
    max_active_runs=1,                  # Only one run at a time
    tags=["collection", "production", "priority-1"],
) as dag:

    # ---- Start ----
    start = EmptyOperator(task_id="start")
    
    # ---- Initialize infrastructure ----
    init_db = PythonOperator(
        task_id="initialize_db_and_storage",
        python_callable=task_initialize_db,
    )
    
    # ---- Collection Task Groups (one per source) ----
    # Each source runs independently — failure in one doesn't block others
    
    all_store_tasks = []
    
    with TaskGroup("collect_jobstreet", tooltip="JobStreet collection for MY and SG") as collect_jobstreet_group:
        for country_code in ["MY", "SG"]:
            collect_task = PythonOperator(
                task_id=f"collect_jobstreet_{country_code}",
                python_callable=task_collect_source,
                op_kwargs={"source_name": "jobstreet", "country_code": country_code},
            )
            
            normalize_task = PythonOperator(
                task_id=f"normalize_jobstreet_{country_code}",
                python_callable=task_normalize_and_store,
                op_kwargs={"source_name": "jobstreet", "country_code": country_code},
            )
            
            collect_task >> normalize_task
            all_store_tasks.append(normalize_task)
    
    # Future task groups (to be added in subsequent sprints):
    # with TaskGroup("collect_linkedin"): ...
    # with TaskGroup("collect_techinasia"): ...
    
    # ---- Compute Snapshots (runs after all collection) ----
    compute_snapshots = PythonOperator(
        task_id="compute_weekly_snapshots",
        python_callable=task_compute_weekly_snapshots,
        trigger_rule="all_done",  # Run even if some collection tasks failed
    )
    
    # ---- Health Check ----
    health_check = PythonOperator(
        task_id="pipeline_health_check",
        python_callable=task_pipeline_health_check,
        trigger_rule="all_done",
    )
    
    # ---- End ----
    end = EmptyOperator(task_id="end")
    
    # ---- DAG Dependencies ----
    start >> init_db >> collect_jobstreet_group
    collect_jobstreet_group >> compute_snapshots >> health_check >> end
