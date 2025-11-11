import argparse
from tabulate import tabulate
import sys
import glob
import io
import logging
import os
import traceback
from executor_utils import setup_executor_logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode, sum as spark_sum
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
)
import numpy as np
from PIL import Image

handlers = [
    logging.StreamHandler(sys.stderr),  # Terminal output
    logging.FileHandler('/opt/spark/spark-logs/image_analysis_driver.log')  # File output
]

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [DRIVER-%(process)d] [%(levelname)s] %(message)s',
    handlers=handlers,
    force=True
)

logger = logging.getLogger(__name__)


def create_spark_session():
    """Create Spark session for distributed image processing"""
    return (
        SparkSession.builder.appName("Distributed_Image_Colour_Analysis")
        .master("spark://spark-master:7077")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "2")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.eventLog.enabled", "true")
        .config("spark.executorEnv.PYTHONUNBUFFERED", "1")
        .config("spark.eventLog.dir", "/opt/spark/spark-logs")
        .getOrCreate()
    )


def analyze_single_image(image_bytes, reduce_colours=True):
    """
    Analyze colour coverage for a single image
    This function will be executed on Spark workers

    Args:
        image_bytes: Image data as bytes
        reduce_colours: Whether to quantize colours

    Returns:
        List of (colour_hex, pixel_count) tuples
    """
    try:
        # Load image from bytes
        img = Image.open(io.BytesIO(image_bytes))
        img = img.convert("RGB")

        # Convert to numpy array
        img_array = np.array(img)
        pixels = img_array.reshape(-1, 3)

        if reduce_colours:
            # Quantize to reduce similar colours
            pixels = (pixels // 16) * 16

        # Count colours efficiently
        unique_colours, counts = np.unique(pixels, axis=0, return_counts=True)

        # Convert to hex and create result list
        results = []
        for colour, count in zip(unique_colours, counts):
            hex_colour = "#{:02x}{:02x}{:02x}".format(colour[0], colour[1], colour[2])
            results.append(
                {
                    "hex_colour": hex_colour,
                    "rgb": f"({colour[0]},{colour[1]},{colour[2]})",
                    "pixel_count": int(count),
                }
            )

        return {"colours": results, "total_pixels": int(len(pixels))}

    except Exception as e:
        logger.error(f"Error processing image: {str(e)}")
        return []


COLOUR_RESULTS_SCHEMA = StructType(
        [
            StructField(
                "colours",
                ArrayType(
                    StructType(
                        [
                            StructField("hex_colour", StringType(), False),
                            StructField("rgb", StringType(), False),
                            StructField("pixel_count", IntegerType(), False),
                        ]
                    )
                ),
                False,
            ),
            StructField("total_pixels", IntegerType(), False),
        ]
    )


def log_header(str, logger):
    logger.info("=" * 70)
    logger.info(str)
    logger.info("=" * 70)


def process_images_batch(spark, image_paths, args):
    """
    Process multiple images in parallel using Spark

    Args:
        spark: SparkSession
        image_paths: List of image folders (can be S3, HDFS, or local)
        output_path: Where to save results (optional)

    Returns:
        DataFrame with aggregated colour statistics
    """

    # Create DataFrame with image paths
    images_df = spark.createDataFrame([(path,) for path in image_paths], ["image_path"])

    colour_schema = COLOUR_RESULTS_SCHEMA

    # UDF to read and analyze images
    @udf(returnType=colour_schema)
    def analyze_image_udf(path):

        exec_logger = setup_executor_logger()
        try:
            # Read image file (works with local, S3, HDFS)
            exec_logger.info(f"Processing image: {path}")
            with open(path, "rb") as f:
                image_bytes = f.read()
            return analyze_single_image(image_bytes, args.reduce_colours)
        except Exception as e:
            exec_logger.error(f"Failed to process {path}: {str(e)}")
            return []

    # TODO Ensure this work is split across spark workers - unsure if withColumn creates partitions that spark can operate on

    # Apply analysis to each image in parallel
    logger.info("Analyzing colours across all images...")

    # Repartition for load balancing
    num_executors = int(spark.conf.get("spark.executor.instances", "2"))
    cores_per_executor = int(spark.conf.get("spark.executor.cores", "2"))
    total_cores = num_executors * cores_per_executor

    # target_partitions = max(total_cores * 2, len(image_paths) // 10)
    target_partitions = 4

    logger.info(
        f"Repartitioning to {target_partitions} partitions for parallel processing"
    )
    logger.info(
        f"Cluster has {num_executors} executors with {cores_per_executor} cores each = {total_cores} total cores"
    )

    images_df = images_df.repartition(target_partitions)
    colours_df = images_df.withColumn(
        "image_data", analyze_image_udf(col("image_path"))
    )

    # Explode colours array to get one row per colour per image
    colours_exploded = colours_df.select(
        col("image_path"), explode(col("image_data.colours")).alias("colour_data")
    ).select(
        col("image_path"),
        col("colour_data.hex_colour"),
        col("colour_data.rgb"),
        col("colour_data.pixel_count"),
    )

    # logger.info(colours_df.show())
    total_pixel_count = colours_df.select(
        spark_sum(colours_df["image_data.total_pixels"])
    ).collect()[0][0]

    # Aggregate across all images to get global colour distribution
    logger.info("Aggregating colour statistics...")

    global_colours = (
        colours_exploded.groupBy("hex_colour", "rgb")
        .agg(spark_sum("pixel_count").alias("total_pixel_count"))
        .withColumn(
            "coverage_percentage", (col("total_pixel_count") / total_pixel_count * 100)
        )
        .orderBy(col("coverage_percentage").desc())
    )

    # Per-image statistics
    per_image_stats = colours_exploded.groupBy("image_path").agg(
        spark_sum("pixel_count").alias("pixel_count")
    )

    # Show results
    # logger.info("\n" + "=" * 70)
    # logger.info("GLOBAL colour DISTRIBUTION ACROSS ALL IMAGES")
    # logger.info("=" * 70)
    # gc_rows = global_colours.limit(20).collect()
    # for row in gc_rows:
    #     logger.info(f"  {row.hex_colour} - {row.coverage_percentage:.2f}%")

    logger.info("=" * 70)
    logger.info("PER-IMAGE STATISTICS")
    logger.info("=" * 70)
    for row in per_image_stats.collect():
        logger.info(f"  {row.image_path} - {row.pixel_count}")

    # Save results if output path provided
    if args.output_path:
        logger.info(f"\nSaving results to {args.output_path}...")
        global_colours.write.mode("overwrite").parquet(f"{args.output_path}/global_colours")
        per_image_stats.write.mode("overwrite").parquet(
            f"{args.output_path}/per_image_stats"
        )
        colours_exploded.write.mode("overwrite").parquet(
            f"{args.output_path}/detailed_colours"
        )
        logger.info("Results saved successfully!")

    return global_colours, per_image_stats, colours_exploded


def create_args_parser():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Distributed image colour analysis with Spark",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "input_path", help="Path to images directory"
    )

    parser.add_argument(
        "output_path",
        nargs="?",
        default=None,
        help="Output path for results (optional)",
    )

    parser.add_argument(
        "--top-colours",
        type=int,
        default=10,
        help="Number of top colours to display (default: 10)",
    )

    parser.add_argument(
        "--reduce-colours",
        action="store_true",
        default=True,
        help="Quantize similar colours together (default: True)",
    )

    parser.add_argument(
        "--file-pattern", default="*.jpg", help="File pattern to match (default: *.jpg)"
    )

    return parser


def main():

    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("INFO")

    parser = create_args_parser()

    try:

        args = parser.parse_args()
        
        logger.info("Starting Distributed Image Colour Analysis")
        logger.info("=" * 70)
        logger.info(f"Input path: {args.input_path}")
        logger.info(f"Output path: {args.output_path or 'None (no output)'}")
        logger.info(f"Top colours: {args.top_colours}")
        logger.info(f"File pattern: {args.file_pattern}")
        logger.info(f"Reduce colours: {args.reduce_colours}")
        logger.info("=" * 70)

        # Local filesystem
        pattern = os.path.join(args.input_path, args.file_pattern)
        image_paths = glob.glob(pattern)

        # Also check for other common image formats
        for ext in ["*.png", "*.jpeg", "*.gif", "*.bmp"]:
            if ext != args.file_pattern:
                image_paths.extend(glob.glob(os.path.join(args.input_path, ext)))

        logger.info(f"Found {len(image_paths)} images matching pattern")

        if not image_paths:
            logger.error(f"No images found in {args.input_path}")
            logger.info("Make sure images exist and pattern is correct")
            return


        # Process images in parallel
        global_colours, per_image, detailed = process_images_batch(
            spark,
            image_paths,
            args
        )

        log_header("Analysis complete!", logger)

        # Show top colours globally

        log_header(f"Top {args.top_colours} colours:", logger)
        
        gc_rows = global_colours.select(
                "hex_colour", "rgb", "total_pixel_count", "coverage_percentage"
                ).sort(["coverage_percentage"], ascending=False).limit(args.top_colours).collect()
            
        zipped_rows = [[row.hex_colour, row.rgb, row.total_pixel_count, f'{row.coverage_percentage:.1f}%'] for row in gc_rows]
        logger.info('\n'+tabulate(zipped_rows, headers=["Hex Colour", "RGB", "Total Pixel Count", "Coverage %"]))

    except Exception as e:
        logger.error(f"Error in pipeline: {str(e)}")

        traceback.print_exc()

    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
