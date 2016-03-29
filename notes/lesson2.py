Data Wrangling - data extraction, data cleaning

# Get data from files, databases, API

# Messing Data - missing data, unmeaningful data
# 1. understand the structure of the data
# 2. common data formats: CSV, XML, JSON
#    - CSV
		import pandas
		baseball_data = pandas.read_csv('master.csv')
		print baseball_data['name_first']

		baseball_data.to_csv('baseball_data2.csv')

#################################################################################
import pandas

def add_full_name(path_to_csv, path_to_new_csv):
    #Assume you will be reading in a csv file with the same columns that the
    #Lahman baseball data set has -- most importantly, there are columns
    #called 'nameFirst' and 'nameLast'.
    #1) Write a function that reads a csv
    #located at "path_to_csv" into a pandas dataframe and adds a new column
    #called 'nameFull' with a player's full name.
    #
    #For example:
    #   for Hank Aaron, nameFull would be 'Hank Aaron', 
	#
	#2) Write the data in the pandas dataFrame to a new csv file located at
	#path_to_new_csv

    #WRITE YOUR CODE HERE
    df = pandas.read_csv(path_to_csv)
    df['nameFull'] = df['nameFirst'] + " " + df['nameLast']
    df.to_csv(path_to_new_csv);



if __name__ == "__main__":
    # For local use only
    # If you are running this on your own machine add the path to the
    # Lahman baseball csv and a path for the new csv.
    # The dataset can be downloaded from this website: http://www.seanlahman.com/baseball-archive/statistics
    # We are using the file Master.csv
    path_to_csv = ""
    path_to_new_csv = ""
    add_full_name(path_to_csv, path_to_new_csv)

#################################################################################

   # - relational database
   # 1. It is straight forward to extract aggregated data with complex files
   # 2. A database scales well
   # 3. It ensure all data is consistently formatted
   # Schema - blue print (column, type)

   SELECT * FROM aadhoar_data; # select all data in the table
   SELECT * FROM aadhoar_data LIMIT 20; # select the first 20 rows
   SELECT district, subdistrict FROM aadhoar_data LIMIT 20; 
   SELECT * FROM aadhoar_data WHERE state = 'Gujarat';
   SELECT district, sum(addhoar_generated) FROM aadhoar_data GROUP BY district;
   SELECT district, subdistrict, sum(aadhoar_generated) FROM aadhoar_data WHERE age > 60 GROUP BY district, subdistrict;


#################################################################################

import pandas
import pandasql

def select_first_50(filename):
    # Read in our aadhaar_data csv to a pandas dataframe.  Afterwards, we rename the columns
    # by replacing spaces with underscores and setting all characters to lowercase, so the
    # column names more closely resemble columns names one might find in a table.
    aadhaar_data = pandas.read_csv(filename)
    aadhaar_data.rename(columns = lambda x: x.replace(' ', '_').lower(), inplace=True)

    # Select out the first 50 values for "registrar" and "enrolment_agency"
    # in the aadhaar_data table using SQL syntax. 
    #
    # Note that "enrolment_agency" is spelled with one l. Also, the order
    # of the select does matter. Make sure you select registrar then enrolment agency
    # in your query.
    #
    # You can download a copy of the aadhaar data that we are passing 
    # into this exercise below:
    # https://www.dropbox.com/s/vn8t4uulbsfmalo/aadhaar_data.csv
    
    q = """
    -- YOUR QUERY HERE
    SELECT registrar, enrolment_agency FROM aadhaar_data LIMIT 50;

    """
    
    #Execute your SQL command against the pandas frame
    aadhaar_solution = pandasql.sqldf(q.lower(), locals())
    return aadhaar_solution

#################################################################################

import pandas
import pandasql

def aggregate_query(filename):
    # Read in our aadhaar_data csv to a pandas dataframe.  Afterwards, we rename the columns
    # by replacing spaces with underscores and setting all characters to lowercase, so the
    # column names more closely resemble columns names one might find in a table.
    
    aadhaar_data = pandas.read_csv(filename)
    aadhaar_data.rename(columns = lambda x: x.replace(' ', '_').lower(), inplace=True)

    # Write a query that will select from the aadhaar_data table how many men and how 
    # many women over the age of 50 have had aadhaar generated for them in each district.
    # aadhaar_generated is a column in the Aadhaar Data that denotes the number who have had
    # aadhaar generated in each row of the table.
    #
    # Note that in this quiz, the SQL query keywords are case sensitive. 
    # For example, if you want to do a sum make sure you type 'sum' rather than 'SUM'.
    #

    # The possible columns to select from aadhaar data are:
    #     1) registrar
    #     2) enrolment_agency
    #     3) state
    #     4) district
    #     5) sub_district
    #     6) pin_code
    #     7) gender
    #     8) age
    #     9) aadhaar_generated
    #     10) enrolment_rejected
    #     11) residents_providing_email,
    #     12) residents_providing_mobile_number
    #
    # You can download a copy of the aadhaar data that we are passing 
    # into this exercise below:
    # https://www.dropbox.com/s/vn8t4uulbsfmalo/aadhaar_data.csv
    
    # your code here     
     q = """SELECT gender, district, sum(aadhaar_generated) FROM aadhaar_data WHERE age > 50 GROUP BY gender, district;"""

    # Execute your SQL command against the pandas frame
    aadhaar_solution = pandasql.sqldf(q.lower(), locals())
    return aadhaar_solution 

#################################################################################


# - API (Application Programming Interface) type: Representational State Transfer
# url: 

# - JSON (pyton dictionary)
url = 'http://ws.audioscrobblers.com/2.0/?/...'
data = requests.get(url).text
print type(data)
print data

import json
import requests

data = requests.get(url).text
data = json.loads(data)
print type(data)
print data
data['artist']


#################################################################################
import json
import requests

def api_get_request(url):
    # In this exercise, you want to call the last.fm API to get a list of the
    # top artists in Spain. The grader will supply the URL as an argument to
    # the function; you do not need to construct the address or call this
    # function in your grader submission.
    # 
    # Once you've done this, return the name of the number 1 top artist in
    # Spain. 
    
    data = requests.get(url).text
    data = json.loads(data)
    
    return data['topartists']['artist'][0]['name'] # return the top artist in Spain

#################################################################################
# sanity checking data
# - does the data makes sense?
# - is there a problem?
# - does the data look like expect it?

>>> import pandas
>>> baseball = pandas.read_csv('../data/Master.csv')
>>> baseball.describe() # quick checking

# Why data missing?
# occasional system errors prevent data from being recorded
# some subset of subject or event types are systematically missing certain data attributes or missing entirely.

# Dealing missing data
# - partial deletion
# 1. Listwise Deletion
# 2. Pairwise Deletion
# - impution
# . why? Not much data; remaining data could affect representationess
# . Many different imputation techniques
# . imputations is really hard to get 
# some methods: 
1. mean: good: doesn't change mean; bad: lessens corrections between validates;
2. Linear regression: data we have -> make an equation to predict variable with missiong values -> predict missing values
  drabacks: over emphasize trands; exact values suggest too muhc certainty

Tip of the imputation:
- just the tip of the iceberg; more sophisticated methods exist
- fill in mean
- linear regression
- both have negative side effects and can observe or amplify trands in data


#################################################################################
import pandas
import numpy

def imputation(filename):
    # Pandas dataframes have a method called 'fillna(value)', such that you can
    # pass in a single value to replace any NAs in a dataframe or series. You
    # can call it like this: 
    #     dataframe['column'] = dataframe['column'].fillna(value)
    #
    # Using the numpy.mean function, which calculates the mean of a numpy
    # array, impute any missing values in our Lahman baseball
    # data sets 'weight' column by setting them equal to the average weight.
    # 
    # You can access the 'weight' colum in the baseball data frame by
    # calling baseball['weight']

    baseball = pandas.read_csv(filename)
    
    #YOUR CODE GOES HERE
    value = numpy.mean(baseball['weight'])
    baseball['weight'] = baseball['weight'].fillna(value)

    return baseball

#################################################################################
#################################################################################
#################################################################################
#################################################################################
#################################################################################
#################################################################################
#################################################################################
#################################################################################
#################################################################################
#################################################################################
#################################################################################
#################################################################################
#################################################################################