#!/bin/bash

# Set the Spark home directory
SPARK_HOME="C:\spark3"

# Set the application JAR file and main class
APP_JAR="C:\Users\ILAYA BHARATHI M\Desktop\231217\IlayaBharathi260199\SparkDSL\target\scala-2.12\sparkdsl_2.12-1.0"
#MAIN_CLASS="DSL.Filter"


# Set other Spark-submit options
SPARK_SUBMIT_OPTIONS="--master local --deploy-mode client --num-executors 2 --executor-memory 2g"

# Run spark-submit command
$SPARK_HOME/bin/spark-submit $SPARK_SUBMIT_OPTIONS --class DSL.joins  $APP_JAR
