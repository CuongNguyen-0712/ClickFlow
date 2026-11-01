# MinIO + Spark without the Kaggle dataset

Scope: implement the screenshot task using the existing Spark 3.5.3,
Delta 3.2.1, Hadoop 3.3.4 and contracts C1/C4/C5. The user requested
execution if possible without a local dataset.

1. Configure the master and two workers with readiness checks, environment
   credentials, mounted contracts, and a storage-only startup command.
2. Initialize configured S3A prefixes and empty partitioned Delta tables
   from C4/C5, preserving data and rejecting incompatible existing tables.
3. Read CSV with explicit C1 types and UTC format. Provide a small synthetic
   fixture and a smoke job that uploads it to a separate raw/csv key, checks
   exact values (including 19-digit IDs), and round-trips Delta in a unique
   temporary prefix. Never impersonate the real 2019-Nov.csv dataset.
4. Verify repeat initialization, schema/nullability/partition contracts,
   malformed CSV rejection, Compose validation, image build, two registered
   workers, and a smoke job submitted from spark-master. Document commands
   for PowerShell and Make, and distinguish synthetic from real-data evidence.

Implementation choices: use Hadoop S3A for storage operations and Delta's
create-if-absent API for empty tables. Run the smoke job on the standalone
cluster. Do not require Kafka or download the Kaggle dataset for this task.

Environment at start: clean main checkout, no local dataset, no working host
Python, Docker Desktop installed but Engine stopped. Start Docker to attempt
real integration verification; record any remaining environmental blocker.

## Verification outcome — 2026-10-29

- Final image build and Compose validation succeeded.
- MinIO and all three Spark services healthy; master reports two ALIVE
  workers, each with 2 cores and 2048 MB.
- Full pytest suite: 5 passed. Spark integration suite: 4 passed.
- Synthetic smoke: 3 rows, C1 CSV via S3A and Delta round-trip passed on
  spark://spark-master:7077. Temporary test objects removed by the jobs.
- Runtime fixes: normalize Delta table paths without trailing slash;
  qualify local upload paths as file URIs for Hadoop 3.3.4.
- Independent review: fixed order-dependent comparison using row multisets;
  fixture now includes repeated product IDs. No remaining important findings.
- Real 2019-Nov.csv validation deferred until dataset is uploaded. No Kaggle
  data downloaded and no synthetic data written to the production tables.
