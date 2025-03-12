from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def initialize_spark(app_name="Task2_Valued_No_Suggestions"):
    """
    Initialize and return a SparkSession.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def load_data(spark, file_path):
    """
    Load the employee data from a CSV file into a Spark DataFrame.
    """
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df

def identify_valued_no_suggestions(df):
    """
    Find employees who feel valued but have not provided suggestions and calculate their proportion.
    """
    valued_no_suggestions = df.filter((col("SatisfactionRating") >= 4) & (col("ProvidedSuggestions") == False))
    total_employees = df.count()
    valued_no_suggestions_count = valued_no_suggestions.count()
    proportion = (valued_no_suggestions_count / total_employees) * 100
    
    return valued_no_suggestions_count, round(proportion, 2)

def write_output(number, proportion, output_path):
    """
    Write the results to a text file.
    """
    with open(output_path, 'w') as f:
        f.write(f"Number of Employees Feeling Valued without Suggestions: {number}\n")
        f.write(f"Proportion: {proportion}%\n")

def main():
    """
    Main function to execute Task 2.
    """
    spark = initialize_spark()
    input_file = "input/employee_data.csv"
    output_file = "outputs/valued_no_suggestions.txt"
    df = load_data(spark, input_file)
    number, proportion = identify_valued_no_suggestions(df)
    write_output(number, proportion, output_file)
    spark.stop()

if __name__ == "__main__":
    main()