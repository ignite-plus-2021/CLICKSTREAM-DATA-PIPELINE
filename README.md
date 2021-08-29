#Clickstream Data Pipeline

_Contains code and other configurations required to process clickstream data_

### Source
Source of data for this pipeline is the clickstream and item dataset in the CSV format.

### Destination
The transformed data is finally written to a mySQL table using spark JDBC. Below is the production table:

```
ignite_prod.clickstream
```

### About the Code

The code performs various cleansing operations (trim, case conversion, elimination NULLs and duplicates) and transformation (joins). It loads the transformed data to a stage table in mySQL, on top of which data quality checks are performed. When the checks pas, the data is finally written to production table for users to consume.




