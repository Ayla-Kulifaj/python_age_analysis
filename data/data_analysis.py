# importing the class pandas so we can use these modules
import pandas as pd
import numpy as np
# Import visualization libraries
import seaborn as sns
import matplotlib.pyplot as plt
import os

# Get the current working directory
current_dir = os.getcwd()

# Assuming 'current_dir' is your 'python_data_analysis' directory,
# construct path to dataset
data_path = os.path.join(current_dir, "data", "AgeDataset.csv")

# Load your dataset
df = pd.read_csv(data_path)

##########################
# Understanding the data #
##########################
def get_dataframe_statistics(df):
    # verify dataframe object
    print(type(df))
    # see first 10 rows of data
    print(df.head)
    # check number of rows and columns
    print(df.shape)
    # see column NAMEs
    print(df.columns)
    # show basic statistics of dataframe
    print(df.describe())
    # General Information
    print(df.info())

    # Descriptive Statistics for numerical columns
    print(df.describe())

    # Descriptive Statistics for categorical columns
    print(df.describe(include='object'))

# run function
get_dataframe_statistics(df)

#cleaning
def clean_dataframe(df):
    # Handling Missing Values
    # Check for missing values
    print(df.isnull().sum())

    # Fill missing values with a placeholder or an average/median (as appropriate)
    df.fillna({'Column_name': 'value_or_median'}, inplace=True)

    # Or drop rows with missing values
    df.dropna(inplace=True)

    # Duplicate Entries
    # Check for duplicate rows
    print(df.duplicated().sum())

    # Drop duplicate rows
    df.drop_duplicates(inplace=True)
    return df

df = clean_dataframe(df)

####Data Transformation
def transformations(df):
    # Type Conversion (if necessary)
    df['Birth year'] = pd.to_datetime(df['Birth year'], format='%Y', errors='coerce')
    df['Death year'] = pd.to_datetime(df['Death year'], format='%Y', errors='coerce')

    # New Columns (e.g., calculating Age at Death if not already present)
    # This is just an example and might not be applicable since 'Age of death' is already provIDed
    df['Calculated Age of Death'] = df['Death year'].dt.year - df['Birth year'].dt.year

    return df

df = transformations(df)

##### Data Analysis
def analyze(df):
    # Distribution Analysis
    # Histogram of Age of Death
    df['Age of death'].hist()

    # Box Plot of Age of Death
    df.boxplot(column=['Age of death'])

    # Trend Analysis (Example: Analyzing age of death over centuries)
    df['Century of Birth'] = (df['Birth year'].dt.year // 100) + 1
    df.groupby('Century of Birth')['Age of death'].mean().plot(kind='line')

    # Categorical Data Analysis
    # Value counts of GENDER
    print(df['Gender'].value_counts())

    # Bar chart of OCCUPATION distribution
    df['Occupation'].value_counts().plot(kind='bar')
    plt.show()
    return df

df = analyze(df)

####Correlation and Relationships
def correlation(df):
    # Scatter Plot (Age of Death vs. Birth Year as an example)
    df.plot(kind='scatter', x='Birth year', y='Age of death')


    # Select only numeric columns for correlation calculation
    numeric_df = df.select_dtypes(include=[np.number])
    print(numeric_df.columns)
    print(numeric_df.corr())

    # Group Comparisons
    # Average Age of Death by GENDER
    print(df.groupby('Gender')['Age of death'].mean())

    ###visualization
    # Heatmap for Correlation
    plt.figure(figsize=(10, 8))
    sns.heatmap(numeric_df.corr(), annot=True, cmap='coolwarm')
    plt.show()

correlation(df)



from snowflake_connector import SnowflakeConnector

# function to clean column NAMEs
def clean_columns(df):
    new_col_map = {}
    for i in df.columns:
        new_col_map[i] = i.upper().replace(' ','_')
    df.rename(columns=new_col_map, inplace=True)
    return df

df = clean_columns(df)
print(df.columns)
# Usage
# enter your Snowflake credentials
user = "xxxxx"
password = "xxxx"
account = "xxxxx"
warehouse = "COMPUTE_WH"
database = "DATA_ANALYSIS"
schema = "DATASETS"

# Create an instance of the connector
connector = SnowflakeConnector(user, password, account, warehouse, database, schema)


# For simplicity, let's assume we're doing an upsert based on the 'ID' column
# Check if each ID in your DataFrame exists in the target table
for index, row in df.iterrows():
    # Manually prepare values, replacing Python None with SQL NULL
    birth_year = None if pd.isnull(row['BIRTH_YEAR']) else f"'{row['BIRTH_YEAR']}'"
    death_year = None if pd.isnull(row['DEATH_YEAR']) else f"'{row['DEATH_YEAR']}'"
    if pd.notnull(row['NAME']):
        name_with_escaped_quotes = row['NAME'].replace("'", "''")
        name_cleaned = f"'{name_with_escaped_quotes}'"
    else:
        name_cleaned = 'NULL'

    if pd.notnull(row['SHORT_DESCRIPTION']):
        short_description_with_escaped_quotes = row['SHORT_DESCRIPTION'].replace("'", "''").replace(",", "")
        short_description_cleaned = f"'{short_description_with_escaped_quotes}'"
    else:
        short_description_cleaned = 'NULL'
    if pd.notnull(row['OCCUPATION']):
        short_occupation_with_escaped_quotes = row['OCCUPATION'].replace("'", "''").replace(",", "")
        short_occupation_cleaned = f"'{short_occupation_with_escaped_quotes}'"
    else:
        short_occupation_cleaned = 'NULL'
    if pd.notnull(row['COUNTRY']):
        short_country_with_escaped_quotes = row['COUNTRY'].replace("'", "''").replace(",", "")
        short_country_cleaned = f"'{short_country_with_escaped_quotes}'"
    else:
        short_country_cleaned = 'NULL'
    values = [
        f"'{row['ID']}'" if pd.notnull(row['ID']) else 'NULL',
        name_cleaned,
        short_description_cleaned,
        f"'{row['GENDER']}'" if pd.notnull(row['GENDER']) else 'NULL',
        short_country_cleaned,
        short_occupation_cleaned,
        'NULL' if pd.isnull(row['BIRTH_YEAR']) else f"'{row['BIRTH_YEAR'].year}'",
        'NULL' if pd.isnull(row['DEATH_YEAR']) else f"'{row['DEATH_YEAR'].year}'",
        f"'{row['MANNER_OF_DEATH']}'" if pd.notnull(row['MANNER_OF_DEATH']) else 'NULL',
        'NULL' if pd.isnull(row['AGE_OF_DEATH']) else str(row['AGE_OF_DEATH']),
        'NULL' if pd.isnull(row['CALCULATED_AGE_OF_DEATH']) else str(row['CALCULATED_AGE_OF_DEATH']),
        'NULL' if pd.isnull(row['CENTURY_OF_BIRTH']) else str(row['CENTURY_OF_BIRTH'])
    ]
    values_string = ', '.join(values)


    query = f"SELECT COUNT(*) FROM historical_figures WHERE ID = '{row['ID']}'"
    query_result = connector.execute(query)
    result = query_result.fetchone()[0]
    
    if result == 0:
        # Record does not exist, perform an INSERT
        insert_query = f"""
        INSERT INTO historical_figures (ID, NAME, SHORT_DESCRIPTION, GENDER, COUNTRY, OCCUPATION, BIRTH_YEAR, DEATH_YEAR, MANNER_OF_DEATH, AGE_OF_DEATH, CALCULATED_AGE_OF_DEATH, CENTURY_OF_BIRTH)
        VALUES ({values_string})
        """
        connector.execute(insert_query)


# for index, row in df.iterrows():
#     # fix NaN
#     row = row.where(pd.notnull(row), None)

#     query = f"SELECT COUNT(*) FROM historical_figures WHERE ID = '{row['ID']}'"
#     query_result = connector.execute(query)
#     result = query_result.fetchone()[0]
    
#     if result == 0:
#         # Record does not exist, perform an INSERT
#         # Note: Dates are now enclosed in single quotes
#         insert_query = f"""
#         INSERT INTO historical_figures (ID, NAME, SHORT_DESCRIPTION, GENDER, COUNTRY, OCCUPATION, BIRTH_YEAR, DEATH_YEAR, MANNER_OF_DEATH, AGE_OF_DEATH, CALCULATED_AGE_OF_DEATH, CENTURY_OF_BIRTH)
#         VALUES ('{row['ID']}', '{row['NAME']}', '{row['SHORT_DESCRIPTION']}', '{row['GENDER']}', '{row['COUNTRY']}', '{row['OCCUPATION']}', '{row['BIRTH_YEAR']}', '{row['DEATH_YEAR']}', '{row['MANNER_OF_DEATH']}', {row['AGE_OF_DEATH']}, {row['CALCULATED_AGE_OF_DEATH']}, {row['CENTURY_OF_BIRTH']})
#         """
#         connector.execute(insert_query)
#     else:
#         # Record exists, perform an UPDATE
#         update_query = f"""
#         UPDATE historical_figures
#         SET NAME = '{row['NAME']}', SHORT_DESCRIPTION = '{row['SHORT_DESCRIPTION']}', GENDER = '{row['GENDER']}', COUNTRY = '{row['COUNTRY']}', OCCUPATION = '{row['OCCUPATION']}', BIRTH_YEAR = '{row['BIRTH_YEAR']}', DEATH_YEAR = '{row['DEATH_YEAR']}', MANNER_OF_DEATH = '{row['MANNER_OF_DEATH']}', AGE_OF_DEATH = {row['AGE_OF_DEATH']}, CALCULATED_AGE_OF_DEATH = {row['CALCULATED_AGE_OF_DEATH']}, CENTURY_OF_BIRTH = {row['CENTURY_OF_BIRTH']}
#         WHERE ID = '{row['ID']}'
#         """
#         connector.execute(update_query)

