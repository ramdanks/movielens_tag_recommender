#!/bin/bash
# About: Movielens Tag Mapper and Ratings Reducer
# Author: Kelompok 2 Big Data (Ramadhan)

MAPRED_MOVIELENS_TAG_PATH="MovielensTagMapper.jar"
RATING_REDUCER_CLASS="RatingReducer"

BOLD="\e[1m"
FAINT="\e[2m"
ITALICS="\e[3m"
UNDERLINED="\e[4m"

YELLOW="\e[33m"
RED="\e[31m"
GREEN="\e[32m"
ENDCOLOR="\e[0m"

source .config

echo "[Environment variable .config]"
echo "HADOOP_HOME=$HADOOP_HOME"
echo "JAVA_HOME=$JAVA_HOME"

echo -e "${YELLOW}[Specify job parameters]${ENDCOLOR}"
read -p "Enter jobname: " jobname
read -p "Enter tag: " tag
read -p "Enter minimum tag count: " tag_count

echo -e "${YELLOW}[Specify working dataset]${ENDCOLOR}"
read -p "Enter hdfs path to tags.csv: " tags_file
read -p "Enter local path to ratings.csv: " ratings_file

echo "Checking..."

hadoop_temp_mapred_path=ItemTagRecommender/${jobname}
output_dir=out
output_path=$(pwd)/${output_dir}/${jobname}
reduced_ratings_path=${output_path}/reduced_ratings.csv

if [ -d "${output_path}" ]; then
    echo "jobname already exists, collision: ${output_path}"
    exit 1
fi

if [ -d "${hadoop_temp_mapred_path}" ]; then
    echo "jobname already exists on hadoop fs, collision: ${hadoop_temp_mapred_path}"
    exit 2
fi

echo "Working..."

$HADOOP_HOME/bin/hadoop jar ${MAPRED_MOVIELENS_TAG_PATH} com.bigdata.tagmap.App ${tag} ${tags_file} ${hadoop_temp_mapred_path}
if [ $? -eq 0 ]; then

    echo -e "${GREEN}Hadoop MapReduce MovielensTag Exited Successfully${ENDCOLOR}"

    if [ ! -d "${output_dir}" ]; then
        mkdir -p ${output_dir}
    fi

    $HADOOP_HOME/bin/hadoop fs -get ${hadoop_temp_mapred_path} ${output_dir}

    cd RatingReducer

    $JAVA_HOME/bin/java ${RATING_REDUCER_CLASS} ${tag_count} ${output_path}/part-r-00000 ${ratings_file} ${reduced_ratings_path}
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}Rating Reduced Successfully${ENDCOLOR}"
    fi

    cd ..

fi

$HADOOP_HOME/bin/hadoop fs -rm -r ${hadoop_temp_mapred_path}
echo "output file=${output_path}"
