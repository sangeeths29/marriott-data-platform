# AWS deployment record

This page documents the personal Marriott Hospitality Data Platform rebuild. It
does not document a Deloitte client deployment. Reservation data is simulated;
the project also supports public hotel reviews, property locations, and financials.

## Deployment status and evidence

The project README records an end-to-end **hybrid AWS deployment**, followed by
teardown to stop costs. Historical results below are reported in that README;
raw execution logs and screenshots were not available when this page was prepared.
No deployment or query was rerun to prepare this document.

| Item | Recorded outcome | Evidence currently available |
|---|---|---|
| AWS storage | Data stored in S3 | README deployment report; Terraform configuration |
| Catalog | Parquet and Iceberg tables in Glue Data Catalog | README deployment report; catalog and registration code |
| Query and gold models | Athena queries and dbt execution | README deployment report; SQL models and dbt configuration |
| Reservation reconciliation | 78,190 reservations; reported exact agreement with local results | README report; original reconciliation output not attached |
| dbt validation | All 47 models/tests reported passing on Athena | README report; original dbt run output not attached |
| Monitoring | Metrics published to CloudWatch | README report; monitoring configuration |
| Teardown | Application stack reported destroyed | README report; original destroy summary not attached |

Infrastructure code shows what the project is configured to deploy. Execution
logs, query exports, and screenshots provide separate evidence that it ran.
Resource totals and the current status of bootstrap resources remain unverified
without the original Terraform summaries or a current account inspection.

## What ran where

| Component | Execution location |
|---|---|
| Data files | AWS S3 |
| Table metadata | AWS Glue Data Catalog |
| SQL queries and dbt gold transformations | AWS Athena |
| Metrics | AWS CloudWatch |
| Spark transformations | Local laptop, reading and writing AWS data |

The README reports that Glue ETL execution was unavailable in the AWS account.
Spark therefore ran locally. Glue ETL job definitions exist in Terraform, but
this recorded run does **not** demonstrate Glue-hosted Spark execution.
MWAA is not provisioned by this repository's Terraform.

## Reproduction commands

These are the repository's documented commands, not a preserved terminal history.
They require AWS credentials, dependencies, environment configuration, and any
necessary account permissions. They create billable resources; execution time
and cost depend on the account and workload.

```bash
# One-time bootstrap, if not already present
make bootstrap

# Plan and deploy the development environment
make tf-plan ENV=dev
make tf-apply ENV=dev

# Configure hybrid execution
make hybrid-env
source .env.hybrid

# Example dataset and reservation run
make sample bronze silver DATE=2026-10-31
make crs-init START=2026-11-01
make crs-day DATE=2026-11-01
make reservations DATE=2026-11-01

# Build and test gold models on Athena
cd dbt
dbt build --profiles-dir . --target dev
cd ..

# Tear down the application environment
make tf-destroy ENV=dev
```

Bootstrap has a separate Terraform state and is not removed by the main
environment's destroy command. The commands above do not establish whether
bootstrap resources currently remain in the account.

## Example queries for a future evidence capture

Select the deployed gold database in Athena before running these queries. These
are query examples; no historical result rows are reconstructed here.

```sql
SELECT count(*) AS reservation_count
FROM fct_reservation;

SELECT brand, stay_month, occupancy, adr_usd, revpar_usd
FROM mart_brand_kpis_monthly
ORDER BY stay_month, brand
LIMIT 20;

SELECT run_date, status, rows_read, rows_rejected,
       rows_inserted, rows_updated, rows_deleted
FROM rpt_pipeline_health
ORDER BY run_date DESC
LIMIT 20;
```

A matching count alone is not proof of row-by-row reconciliation. Preserve the
comparison method, mismatch counts, dataset dates, and code commit alongside
any future reconciliation result.

## AWS issues recorded in the README

- Athena could not write timezone-bearing timestamps into Hive tables; staging
  SQL casts them to timestamps without time zones.
- Athena date sequences needed explicit date casts.
- Gold output needed a durable lake location rather than the query-results
  prefix with its short retention period.
- Explicit Parquet schema registration replaced crawler-based discovery for
  the hybrid silver registration path.

See the [README](../README.md), [Terraform](../terraform/),
and [GitHub Actions workflows](../.github/workflows/) for the implementation. Saved CI run links and deployment logs can be added
when available; their historical status is not verified by this page.

## Screenshots and logs to attach

No screenshots are currently attached. When available, store redacted images
under `docs/aws/` and embed them here with descriptive captions:

- S3 lake prefixes and relevant data files.
- Glue Catalog table details showing the reservations table format.
- Athena query text, selected database, and returned KPI rows.
- CloudWatch dashboard with the relevant time range.
- IAM role configuration relevant to CI deployment.
- Terraform apply/destroy summaries and dbt execution summary.

Redact account identifiers and personal details, and exclude credentials,
tokens, Terraform state, and secret values. Preserve dates and commit identifiers
where possible so reviewers can connect evidence to a particular run.

## Interview explanation

> I rebuilt the hospitality pipeline independently using public hotel datasets
> and simulated reservations. I ran it in hybrid mode: Spark executed locally,
> while storage, cataloging, analytics, and monitoring used AWS services. The
> README records matching local/cloud reservation results and a successful dbt
> build. I then tore down the application stack to stop ongoing costs.

Use the reported numerical results with their provenance until the original
execution artifacts are attached.
