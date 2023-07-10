#!/bin/bash

# Specify the paths and filenames
JAR_FILE_ONE="profitOneMapReduce.jar"
JAR_FILE_MULTIPLE="profitMultipleMapReduce.jar"
INPUT_FOLDER="$2"
OUTPUT_FOLDER="$3"
PYTHON_SCRIPT="combine.py"

# Function to print help message
print_help() {
    echo "Usage: $0 <option> <input_folder> <output_folder>"
    echo ""
    echo "Options:"
    echo "  1      Use $JAR_FILE_ONE"
    echo "  2      Use $JAR_FILE_MULTIPLE"
    echo "  -help  Print this help message"
    echo ""
    echo "Example:"
    echo "  $0 1 input_folder output_folder"
}

# Check which jar file to use based on the first argument
if [ "$1" = "1" ]; then
    JAR_FILE="$JAR_FILE_ONE"
elif [ "$1" = "2" ]; then
    JAR_FILE="$JAR_FILE_MULTIPLE"
elif [ "$1" = "-help" ]; then
    print_help
    exit 0
else
    echo "Invalid argument. Please specify 1 for profitOneMapReduce.jarI apologize for the cut-off in the previous message. Here's the complete modified shell script:"


# Specify the paths and filenames
JAR_FILE_ONE="profitOneMapReduce.jar"
JAR_FILE_MULTIPLE="profitMultipleMapReduce.jar"
INPUT_FOLDER="$2"
OUTPUT_FOLDER="$3"
PYTHON_SCRIPT="combine.py"

# Function to print help message
print_help() {
    echo "Usage: $0 <option> <input_folder> <output_folder>"
    echo ""
    echo "Options:"
    echo "  1      Use $JAR_FILE_ONE"
    echo "  2      Use $JAR_FILE_MULTIPLE"
    echo "  -help  Print this help message"
    echo ""
    echo "Example:"
    echo "  $0 1 input_folder output_folder"
}

# Check which jar file to use based on the first argument
if [ "$1" = "1" ]; then
    JAR_FILE="$JAR_FILE_ONE"
elif [ "$1" = "2" ]; then
    JAR_FILE="$JAR_FILE_MULTIPLE"
elif [ "$1" = "-help" ]; then
    print_help
    exit 0
else
    echo "Invalid argument. Please specify 1 for profitOneMapReduce.jar or 2 for profitMultipleMapReduce.jar."
    exit 1
fi

# Run the Python script to combine the CSV files
python $PYTHON_SCRIPT $INPUT_FOLDER

# Load the combined CSV file onto Hadoop filesystem
hadoop fs -mkdir $INPUT_FOLDER
hadoop fs -put $INPUT_FOLDER/combinedDataset.csv /input

# Check if the specified jar file exists in the current directory
if [ -f "$JAR_FILE" ]; then
    # Jar file found, directly run the Hadoop job
    hadoop jar $JAR_FILE ProfitDriver /input $OUTPUT_FOLDER
else
    # Jar file not found, print an error message
    echo "Jar file $JAR_FILE not found."
    exit 1
fi
