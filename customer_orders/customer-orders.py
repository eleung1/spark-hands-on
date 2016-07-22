# Given customer order CSV file, find the amount spent
# per customer.
#
# CSV format: CustomerID,ItemID,price
#
# Result format: CustomerID,totalSpent
#
# Author: Eric Leung
#


from pyspark import SparkConf, SparkContext

# Helper method that extracts the customerId and price from the given line
# arg: line is a comma separated string with format: customerId,itemId,price
# ret: A 2-tuple (customerId, price)
def extractCustIdAndPrice(line):
    fields = line.split(',')
    custId = int(fields[0])
    price = float(fields[2])
    return (custId, price)

# Boiler plate setup for local instance of Spark
conf = SparkConf().setMaster("local").setAppName("CustomerOrders")
sc = SparkContext(conf = conf)

# Create a RDD (Resilient Distributed Dataset) by reading the CSV file
lines = sc.textFile("./customer-orders.csv")

# Map function extracts customerIds and prices from the raw CSV entries
# Reduce function add the prices together for entries with same customerID
# Flip custId and price so we can sort by key (i.e. price)
#
# custIdAndAmountSpent is another RDD
custIdAndAmountSpent = (lines.map(extractCustIdAndPrice).reduceByKey(lambda x, y: x + y)
                                                        .map(lambda (x,y): (y,x))
                                                        .sortByKey())

# Collect the results.
results = custIdAndAmountSpent.collect()

# Print results out to console
for result in results:
    print str(result[1]) + "\t{:10.2f}".format(result[0])