# Introduction
This is a script to upload the content of a Data Sample into Google Spanner. 
The idea is to write a code that is capable of uploading an Excel file with 3 tables to the database system. After that, we built a visualization of the data contained in that file
using Looker Studio.

You can find that report at the next public link: https://lookerstudio.google.com/reporting/18ee5d1c-500d-40e6-8720-c35f8153f3d2

# Data set
The set used is a Store Data set that has sales transactional fake data. The main reason for using this set is that the possible visualizations for this set are known, the author has personal experience working with it and offers a good contrast between what can be built with it in comparison with other tools.

You will find the Excel file as sample_superstore.xlsx.

# Python Script
The script has 2 classes that do the integration work. *OrdersDataFrame* turns the Excel file into a data frame that allows us to filter and make some calculations over it to make a cross check and avoid mismatches between the different data stages.

The second class is the *GoogleSpannerDB*. This one is responsible for the connection with the database, the DDL that builds the database and the upload of the information.

# Looker Studio Report
The report has 3 pages and each one has a different purpose; we are going to review every one of them.

## 2025 Sales report (actual year)
The main objective of this view is to visualize the sales of the current year. It has limited interactions but you can filter the elements inside it using the tree map that disaggregates the sales using the category and subcategory dimensions.

One of the most important features is that it allows the user to compare the sales to date during the current year with the sales goal. It has a dynamic color configuration that changes the color of the difference when it is positive to green and keeps it in red until the goal is reached.

This report has some limitations. The ideal situation would be to calculate the goal and the previous year difference based on the selections of different variables such as category or segment. But to do that, we would need to build a table that has these metrics and dimensions in the same row. That is totally possible to implement, but it was preferred to not do it because of time margin limitations.

## Sales by state report
The purpose of this report is to show the amount sold in a date range between the different states of the USA. You will see 2 metrics in the graph. Sales, represented by the size of the bubble, and profit, which is represented by the color of the bubble.

The color representation was limited in Looker Studio. To do this we had to make a calculated field using this formula:
```
CASE
  WHEN Profit > 0 THEN "Positive"
  WHEN Profit < 0 THEN "Negative"
  ELSE "Zero"
END
```
That allows us to make a discrete color scale with 0 as the mid value.

The page has filters to see how the relation changes depending on the customer and the product category.

## Sales trend report
This page aims to show how sales grow from 2022 to 2025. For that reason, a trend line was added to the visualization.

The report has the possibility to filter the data by location, customer segment and category/subcategory.

