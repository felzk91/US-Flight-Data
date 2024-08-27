# US-Flight-Data
This project conducts an exploratory data analysis to identify insights and patterns for US commercial flights from 1987 to 2008. The analysis leverages SQL for data cleaning and aggregation, Tableau for visualisation, and Python for statistical modelling.

## Data Information
The dataset includes over 120 million records of commercial US flights, which include details such as departue and arrival times, flight delays, plane details and carrier information.

The complete dataset, along with supplementary information and variable descriptions, can be downloaded from the Havard Dataverse at https://doi.org/10.7910/DVN/HG7NV7. 

## Methodology
Due to the large sample size, the raw data is stored in PostgreSQL database. SQL is used to clean the data, transform variables, and creating aggregated tables.

The cleaned and aggregated data is visualised using Tableau. Different analyses are conducted to address questions such as the best time to travel to minimise delays.

Regression analysis is also performed using Python to explore if delays in current flights affect subsequent flights.

The repository includes SQL queries for data cleaning, while the accompanying presentation contains the results and visualisations of the analysis.
