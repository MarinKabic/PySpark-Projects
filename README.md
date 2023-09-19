# PySpark Projects
This repository currently contains 2 PySpark projects:
1. Analyzing Car Data
2. Determining the Least Popular Superhero from Marvel Universe

# PySpark Project 1: Analyzing Car Data

## Overview
This PySpark project focuses on analyzing car data obtained from a web scraping effort. (https://github.com/MarinKabic/CarsFuelEconomy-Python-SQL)
The objective is to determine the most common car year and the count of cars manufactured in that year. The analysis utilizes Apache Spark and PySpark for data processing.

## Dataset
**cars.csv** - contains information about various cars, including their names, type (manual or motor), miles per gallon (MPG), and annual fuel cost.

## Insights and Analysis
The primary goal of this project is to answer the following questions:
- What is the most common car year in the dataset?
- How many cars were manufactured in the most common car year?

### Skills and Techniques Used
To accomplish this analysis, the following skills and techniques were employed:
- Apache Spark: Utilizing distributed computing capabilities for efficient data processing.
- PySpark DataFrames: Working with structured data using PySpark's DataFrame API.
- Data Transformation: Extracting the car year from the "Car Name" column.
- Grouping and Aggregation: Counting the occurrences of each car year and finding the most common year.





# PySpark Project 2: Determining the Least Popular Superhero

## Overview
This PySpark project is focused on analyzing superhero connections data to determine the least popular superhero/superheores (superheroes with the fewest connections). 
The project utilizes Apache Spark and PySpark to process and analyze the data.

## Dataset
The dataset used for this analysis consists of two main files:
- **Marvel-Names.txt**: This file contains superhero IDs and names.
- **Marvel-Graph.txt**: This file establishes connections between superheroes.

## Insights and Analysis
The project aims to answer the following questions:
- Who is the least popular superhero/which superheroes have the least number of connections?
- What are the superhero names with the minimal number of connections?

### Skills and Techniques Used
The following skills and techniques were employed to accomplish the analysis:
- Apache Spark: Leveraging the power of distributed computing for data processing.
- PySpark DataFrames: Working with structured data using PySpark's DataFrame API.
- Data Transformation: Splitting and manipulating data to extract necessary information.
- Aggregation: Calculating the total number of connections for each superhero.
- Join Operations: Combining datasets to associate superhero names with their IDs.



