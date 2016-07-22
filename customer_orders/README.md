#Exercise: Count up total amount ordered by customer

Given CSV file containing customer order entries, find total number of dollars spent for each customer.

CSV format:
```
Customer ID, Item ID, Price
```

For instance, given this CSV:
```
44,8602,37.19
35,5368,65.89
44,3391,40.64
47,6694,14.98
35,680,13.08
```

This is the expected results:
```
44,77.83
35,78.97
47,14.98
```

To run this script:
```
spark-submit customer-orders.py
```
