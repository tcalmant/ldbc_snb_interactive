# Input preparation

## Requirements

### Dataset generation

* sbt (to compile the generator)
* Java 8+
* Spark 2.4.5 (used in local mode)
* 2.2 GB of disk space, times the scale factor

### Benchmark

* Docker 19+
* Python 3.7+
* Java 9 to execute the benchmark. It can be executed in a specific container
* 4.4 GB of disk space, times the scale factor

## Walkthrough

In the current state of this branch, I manage to run a benchmark with the
following steps:

1. Compile [`ldbc/ldbc_snb_datagen_spark`](https://github.com/ldbc/ldbc_snb_datagen_spark)
using `sbt`: `sbt assembly`
1. Generate the CSV dataset by running the assembly JAR from the
   `ldbc_snb_datagen_spark` project, found in the `target/scala-2.11/` folder.
   It is expected to be executed on Spark&nbsp;2.4.5 in local mode:

   ```bash
   scale_factor=1
   spark-submit \
        --master local[*] \
        --conf spark.hadoop.fs.defaultFS=file:${PWD} \
        --conf spark.hadoop.fs.default.name=file:${PWD} \
        --driver-memory 40G \
        --executor-memory 40G \
        --class ldbc.snb.datagen.spark.LdbcDatagen \
        ldbc_snb_datagen-assembly-0.4.0-SNAPSHOT.jar \
        --format csv \
        --scale-factor "$scale_factor" \
        --output-dir "out-scale-$scale_factor-explode-attrs" \
        --explode-attrs \
        --mode interactive
   ```

   **Notes:**
   * I had issues trying to run it in a cluster with HDFS (conflicts due to the use
   of local absolute path in HDFS
   * Scale factor 1 takes around 2.2&nbsp;GB of disk space
   * This has been executed on Spark&nbsp;2.4.5 with Java 8
1. Compile the [driver library](https://github.com/ldbc/ldbc_snb_driver)
and install in your local Maven repository:
`mvn clean install -DskipTests` (tests fail)
1. Clone this version of the
   [SNB interactive tool](https://github.com/tcalmant/ldbc_snb_interactive)
   repository and checkout its `wip` branch:
   ```bash
   git clone https://github.com/tcalmant/ldbc_snb_interactive.git
   cd ldbc_snb_interactive
   git checkout -b wip wip
   ```
1. Run the CSV preparation script to merge the dataset files and make them
   loadable by PostgreSQL:
   ```bash
   # From the root of the ldbc_snb_interactive project directory
   python3 ./prepare_files.py \
       --ddl ./postgres/ddl \
       /path/to/out-scale-$scale_factor-explode-attrs
   ```

   **Notes:**
   * This script requires Python 3.7+ and the
   [`fuzzywuzzy`](https://pypi.org/project/fuzzywuzzy/) package
   * This will copy the dataset, which you'll need to have enough disk space
   for this script to succeed
1. Move to the `postgres` folder of the `ldbc_snb_interactive` project
1. If necessary, update the `.properties` files in the `driver` folder
to remove the execution of the *update* queries. We won't benchmark against
them and they cause column constraint errors (already done in `wip`).
1. On Windows (or WSL), create a Docker volume to hold the database:
   `docker volume create ldbc_interactive` and update `scripts/vars.sh` with:
   ```bash
   export POSTGRES_DATABASE_DIR=ldbc_interactive
   ```

   On Linux, you can either use a Docker volume or a local folder. Update the
   `POSTGRES_DATABASE_DIR` variable accordingly.
1. If necessary, you can change the Docker container published port by setting
the `POSTGRES_PORT` variable in `scripts/vars.sh`
1. Start the database and load the data:
   ```bash
   ./scripts/start.sh && ./scripts/create-db.sh && ./scripts/load.sh
   ```
1. Run the validation script
   ```bash
   ./driver/create-validation-parameters.sh
   ./driver/validate.sh
   ```
1. Run the benchmark
   ```bash
   ./driver/benchmark.sh
   ```
   **Note:** This requires Java 9 (doesn't work in Java 8 nor Java 17)

   You can use a Docker container to run the benchmark with Java 9:
   ```bash
   docker run -it \
       -v ${PWD}:/host \
       -w /host \
       --net host \
       --entrypoint /bin/bash \
       openjdk:9-jre
   ```
