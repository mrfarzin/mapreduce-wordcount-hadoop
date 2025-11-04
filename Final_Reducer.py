import os
import subprocess
import time

# Start timing
start_time = time.time()

# Remove the previous output file if it exists
local_output_file = "mapreduce_final_result.txt"  # Final reduced result
if os.path.exists(local_output_file):
    os.remove(local_output_file)
    print(f"Removed previous {local_output_file}")

# Automatically find the Hadoop streaming jar
hadoop_streaming_jar = subprocess.check_output(
    "echo $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar",
    shell=True, text=True
).strip()

# HDFS Paths
input_path = "/input"
output_path = "/output"
hdfs_output_file = f"{output_path}/part-00000"
summary_file = "mapreduce_summary.txt"  # File to store the summary

# Directory containing all "mapreduce_result" files
local_input_dir = "Results"  # Replace with the actual directory path

# Pre-job HDFS setup
print("Preparing HDFS directories...")
try:
    # Remove old input/output directories if they exist
    subprocess.run(["hdfs", "dfs", "-rm", "-r", output_path], check=False)
    subprocess.run(["hdfs", "dfs", "-rm", "-r", input_path], check=False)
    
    # Create a fresh input directory
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", input_path], check=True)
    print("HDFS setup completed successfully.")
except subprocess.CalledProcessError as e:
    print(f"Error during HDFS setup: {e}")
    exit(1)

# Upload all "mapreduce_result" files to HDFS with unique names
print(f"Uploading files from {local_input_dir} to HDFS...")
for root, _, files in os.walk(local_input_dir):
    for file in files:
        if file.startswith("mapreduce_final_result"):
            unique_name = f"{os.path.basename(root)}_{file}"
            local_file_path = os.path.join(root, file)
            hdfs_file_path = os.path.join(input_path, unique_name)
            try:
                subprocess.run(["hdfs", "dfs", "-put", local_file_path, hdfs_file_path], check=True)
                print(f"Uploaded: {local_file_path} as {hdfs_file_path}")
            except subprocess.CalledProcessError as e:
                print(f"Failed to upload {local_file_path} to HDFS: {e}")
                exit(1)

# Run the MapReduce job
print("Running MapReduce job...")
try:
    subprocess.run([
        "hadoop", "jar", hadoop_streaming_jar,
        "-mapper", "cat",  # Pass input directly to the reducer
        "-reducer", "reducer.py",  # Use a custom reducer for final aggregation
        "-input", input_path,
        "-output", output_path
    ], check=True)
    print("MapReduce job completed successfully.")
except subprocess.CalledProcessError as e:
    print(f"Error running MapReduce job: {e}")
    exit(1)

# Calculate total processing time
end_time = time.time()
processing_time = end_time - start_time

# Fetch the final reduced result from HDFS
print("Fetching reduced output from HDFS...")
try:
    subprocess.run(["hdfs", "dfs", "-get", hdfs_output_file, local_output_file], check=True)
    print(f"Final reduced result written to: {local_output_file}")
except subprocess.CalledProcessError as e:
    print(f"Error fetching output from HDFS: {e}")
    exit(1)

# Parse the local output file to calculate the total word count
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

# Log the results to the console and a summary file
summary_message = f"""
=== MapReduce Job Summary ===
Total Word Count: {total_word_count}
Total Processing Time: {processing_time:.2f} seconds
Results exported to: {local_output_file}
"""

print(summary_message)
with open(summary_file, "w") as summary_output:
    summary_output.write(summary_message)

print(f"Summary written to: {summary_file}")

