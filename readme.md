# Local Spark Cluster Setup

## Quick Start

1. **Create the project structure:**
```bash
mkdir -p data apps notebooks spark/spark-logs spark/work
```

2. **Start the cluster:**
```bash
docker-compose up -d
```

3. **Check it's running:**
- Spark Master UI: http://localhost:8080
- Spark Application UI: http://localhost:4040 (when running a job)
- Spark History Server: http://localhost:18080
- Jupyter Lab: http://localhost:8888 (no password needed)
- You should see 2 (or however many that have been configured) workers connected

## What You Get

- **Spark Master** - coordinates the cluster
- **2 Spark Workers** - each with 2GB RAM, 2 cores
- **Spark History Server** - view completed application logs and metrics
- **PostgreSQL** - for reading/writing data
- **Kafka + Zookeeper** - for streaming (optional, can comment out)
- **Jupyter Lab** - with PySpark, pandas, and data science libraries pre-installed

## Running Spark Jobs

Once the cluster is up, you can use the below command to run the analysis job. Replace the final argument with the location of your image files.

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 2g \
  --executor-cores 2 \
  --py-files /opt/spark-apps/executor_utils.py \
  /opt/spark-apps/spark_image_analysis.py \
  /opt/spark-data/images/corn/Zea_mays_Chulpi_Cancha
```

For more information on usage of spark_image_analysis.py, here is the result of the --help argument:

```bash
positional arguments:
  input_path            Path to images directory
  output_path           Output path for results (optional)

optional arguments:
  -h, --help            show this help message and exit
  --top-colors TOP_COLORS
                        Number of top colors to display (default: 10)
  --reduce-colors       Quantize similar colors together (default: True)
  --file-pattern FILE_PATTERN
                        File pattern to match (default: *.jpg)
```

## Viewing Cluster Metrics

While your job runs:
- **Master UI (localhost:8080)** - see workers, running applications, resource usage
- **Application UI (localhost:4040)** - see stages, tasks, DAG visualization, shuffle data, executors
- **History UI (localhost:18080)** - see finished applications with memory, core, and partition data

## Useful Commands

**Stop the cluster:**
```bash
docker-compose down
```

**View logs:**
```bash
docker-compose logs -f spark-master
docker-compose logs -f spark-worker-1
```

**Scale workers:**
```bash
docker-compose up -d --scale spark-worker=4
```

**Access Spark shell:**
```bash
docker exec -it spark-master spark-shell --master spark://spark-master:7077
```

**Access PySpark shell:**
```bash
docker exec -it spark-master pyspark --master spark://spark-master:7077
```

## Troubleshooting

- **Port conflicts?** Change the ports in docker-compose.yml
- **Out of memory?** Increase SPARK_WORKER_MEMORY or reduce data size
- **Can't see Application UI?** It only appears while a job is running
- **Workers not connecting?** Check `docker-compose logs spark-master`