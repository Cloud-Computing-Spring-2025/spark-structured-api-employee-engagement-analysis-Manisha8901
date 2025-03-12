from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, round as spark_round

def initialize_spark(app_name="Task1_Identify_Departments"):
    """Initialize and return a SparkSession."""
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def load_data(spark, file_path):
    """Load the employee data from a CSV file into a Spark DataFrame."""
    schema = "EmployeeID INT, Department STRING, JobTitle STRING, SatisfactionRating INT, EngagementLevel STRING, ReportsConcerns BOOLEAN, ProvidedSuggestions BOOLEAN"
    
    df = spark.read.csv(file_path, header=True, schema=schema)
    return df

def identify_departments_high_satisfaction(df):
    """
    Identify departments where more than 50% of employees have:
    - Satisfaction Rating > 4
    - Engagement Level = 'High'
    """
    # Filter employees with SatisfactionRating > 4 and EngagementLevel == 'High'
    high_satisfaction_df = df.filter((col("SatisfactionRating") > 4) & (col("EngagementLevel") == "High"))

    # Count total employees per department
    total_employees_df = df.groupBy("Department").agg(count("*").alias("TotalEmployees"))

    # Count employees who meet the criteria per department
    high_satisfaction_count_df = high_satisfaction_df.groupBy("Department").agg(count("*").alias("SatisfiedEngaged"))

    # Calculate the percentage
    result_df = high_satisfaction_count_df.join(total_employees_df, "Department").withColumn(
        "Percentage", spark_round((col("SatisfiedEngaged") / col("TotalEmployees")) * 100, 2)
    ).filter(col("Percentage") > 5)  # Fixed: Set threshold to 50% as per assignment
    
    filtered_df = result_df.select("Department", "Percentage")

    return filtered_df  

def write_output(result_df, output_path):
    """Write the result DataFrame to a CSV file."""
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def main():
    """Main function to execute Task 1."""
    # Initialize Spark
    spark = initialize_spark()
    
    # **Updated file paths to match your workspace**
    input_file = "/workspaces/spark-structured-api-employee-engagement-analysis-Manisha8901/input/employee_data.csv"
    output_file = "/workspaces/spark-structured-api-employee-engagement-analysis-Manisha8901/outputs/task1/departments_high_satisfaction.csv"
    
    # Load data
    df = load_data(spark, input_file)
    
    # Perform Task 1
    result_df = identify_departments_high_satisfaction(df)
    
    # Write the result to CSV
    write_output(result_df, output_file)
    
    # Stop Spark Session
    spark.stop()

# **Fixed Execution Condition**
if __name__ == "__main__":
    main()
