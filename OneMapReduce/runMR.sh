#!/bin/bash

# Specify the paths and filenames
JAR_FILE="profit.jar"
INPUT_FOLDER="$1"
OUTPUT_FOLDER="$2"

# Check if profit.jar exists in the current directory
if [ -f "$JAR_FILE" ]; then
    # profit.jar found, directly run the Hadoop job
    hadoop jar $JAR_FILE ProfitDriver $INPUT_FOLDER $OUTPUT_FOLDER
else
    # profit.jar not found, compile the code first
    javac -cp "/usr/local/Cellar/hadoop/3.3.4/libexec/share/hadoop/common/*:/usr/local/Cellar/hadoop/3.3.4/libexec/share/hadoop/mapreduce/*" -d . ProfitMapper.java ProfitReducer.java ProfitCombiner.java ProfitDriver.java
    jar -cvf $JAR_FILE *.class

    # Run the Hadoop job
    hadoop jar $JAR_FILE ProfitDriver $INPUT_FOLDER $OUTPUT_FOLDER
fi
