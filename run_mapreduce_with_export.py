import os
import subprocess
import time

# Start timing
start_time = time.time()

# Remove the previous output file if it exists
if os.path.exists("mapreduce_result.txt"):
    os.remove("mapreduce_result.txt")
    print("Removed previous mapreduce_result.txt")

# Automatically find the Hadoop streaming jar
hadoop_streaming_jar = subprocess.check_output(
    "echo $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar",
    shell=True, text=True
).strip()

# Paths
local_input_file = "text.txt"  # Local input file to upload
input_path = "/input"
output_path = "/output"
hdfs_input_file = f"{input_path}/text.txt"
hdfs_output_file = f"{output_path}/part-00000"
local_output_file = "mapreduce_result.txt"  # Local file for exporting results
summary_file = "mapreduce_summary.txt"  # File to store the summary

# Pre-job HDFS setup
print("Preparing HDFS directories...")
try:
    # Remove old output directory (if exists)
    subprocess.run(["hdfs", "dfs", "-rm", "-r", output_path], check=True)
    # Remove old input directory (if exists)
    subprocess.run(["hdfs", "dfs", "-rm", "-r", input_path], check=True)
    # Create input directory
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", input_path], check=True)
    # Upload the local input file to HDFS
    subprocess.run(["hdfs", "dfs", "-put", local_input_file, hdfs_input_file], check=True)
    print("HDFS setup completed successfully.")
except subprocess.CalledProcessError as e:
    print(f"Error during HDFS setup: {e}")
    exit(1)

# Run the MapReduce job
print("Running MapReduce job...")
subprocess.run([
    "hadoop", "jar", hadoop_streaming_jar,
    "-mapper", "mapper.py",
    "-reducer", "reducer.py",
    "-input", input_path,
    "-output", output_path
], check=True)

# Calculate total processing time
end_time = time.time()
processing_time = end_time - start_time

# Check the output folder in HDFS
print("Checking HDFS output directory...")
ls_command = subprocess.run(["hdfs", "dfs", "-ls", output_path], capture_output=True, text=True)
print(ls_command.stdout)

# Copy HDFS output to the local file system
print("Fetching output from HDFS...")
try:
    subprocess.run(["hdfs", "dfs", "-get", hdfs_output_file, local_output_file], check=True)
    print(f"Output successfully fetched to: {local_output_file}")
except subprocess.CalledProcessError:
    print("Error: Unable to fetch output from HDFS. Ensure the MapReduce job wrote output to HDFS.")
    exit(1)

# Parse the local output file to calculate total word count
print("Calculating total word count...")
total_word_count = 0

try:
    with open(local_output_file, "r") as export_file:
        for line in export_file:
            _, count = line.strip().split("\t")
            total_word_count += int(count)
except FileNotFoundError:
    print("Error: Exported file not found. Check if the HDFS file was correctly fetched.")
    exit(1)

# Log the results both to the console and to a text file
summary_message = f"""
=== MapReduce Job Summary ===
Total Word Count: {total_word_count}
Total Processing Time: {processing_time:.2f} seconds
Results exported to: {local_output_file}
"""

# Print to console
print(summary_message)

# Write to summary file
with open(summary_file, "w") as summary_output:
    summary_output.write(summary_message)

print(f"Summary written to: {summary_file}")

