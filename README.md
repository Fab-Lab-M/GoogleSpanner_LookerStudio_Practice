# Introduction
This is a script to upload the content of a Data Sample into Google Spanner. 
The idea is to write a code that is capable of upload an excel file with 3 tables to the database system. After that we built a visualization of the data contained in that file
using Looker Studio.

You can find that report in the next public link: https://lookerstudio.google.com/reporting/18ee5d1c-500d-40e6-8720-c35f8153f3d2

# Data set
The set used is a Store Data set that has sales transactional fake data. The main reason to use this set is because the posible visualization for this set are known, the author has personal experience working with it and offer a good contras between what can be build with it in comparison with other tools.

You will find the excel file as sample_superstore.xlsx.

# Python Script
The script has 2 clases that do the integration work. *OrdersDataFrame* turns the excel file into a data frame that allows us to filter and make some calculations over it to make a cross check and avoid mismatchs between the diferent data stages.

The second class is the *GoogleSpannerDB*. This one is responsible of the connection with the database, the DDL that build the database and the upload of the information.

# Looker Studio Report
The report has 3 pages and each thing has a different purpose we are going to review everyone of them.

## 2025 Sales report (actual year)
The main objective of this view is to visualize the sales of the current year. It has limited interactions but you can filter the elements inside it using the tree map that disaggregates the sales using the category and subcategory dimensions.

One of the most important features is that allows the user to compare the sales to date during the current year with the sales goal. It has a dinamic color configuration that changes the color of the difference when is positive to green and keep it in red until the goal is reached.

This report has some limitations. The ideal situation would be to calculate the goal and the previus year difference based ont he selections of different variables as catagory or segment. But to do that, we would need to build a table that has this metrics and dimensions in the same row. Thats is totally possible to implement, but was prefered to not do it because of time margin limitations.

## Sales by state report
The propouse of this report is to show the amount sold in a date rage between the different states of the USA. You will see 2 metrics in the graph. Sales, represented by the size of the bubble and profit which is represented with the color of the bubble.

The color representation was limited in Looker studio. To do this we had to make a calculated field using this formula:
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
This page wants to show how sales grows from 2022 to 2025. For that reason a trend line was added to the visualization.

The report has the possibility to filter the data by location, customer segment and category/subcategory.

