#!/bin/bash


# Script is suitable for local run
# For cluster run
# 1.change "--deploy-mode cluster",-"-master YARN" in SPARK_SUBMIT_OPTIONS
# 2.Add resources as per data (Static(Manual) or Dynamic(Auto))

# Set the Spark home directory
SPARK_HOME="/home/ubuntu/Downloads/spark-3.4.1-bin-hadoop3/"

# Set the application JAR file and main class

#   =====Change APP_JAR path According to Your Requirement=====
APP_JAR="/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/SparkDSL/target/scala-2.12/sparkdsl_2.12-1.0.jar"

#   =====Change MAIN_CLASS path According to Your Requirement=====
MAIN_CLASS="dsl.Word_count"

#   =====Change JOB_NAME(optional) path According to Your Requirement=====
JOB_NAME="SparkJob"

# Set other Spark-submit options
SPARK_SUBMIT_OPTIONS="--master local --deploy-mode client --num-executors 2 --executor-memory 2g"

# Run spark-submit command
$SPARK_HOME/bin/spark-submit --name $JOB_NAME $SPARK_SUBMIT_OPTIONS --class $MAIN_CLASS $APP_JAR
