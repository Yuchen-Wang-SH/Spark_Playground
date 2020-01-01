from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Customer Total Value")
sc = SparkContext(conf=conf)

def parse_line(line):
    l = line.split(',')
    customer_id = int(l[0])
    value = float(l[2])
    return (customer_id, value)

t = sc.textFile("./customer-orders.csv")
tuples = t.map(parse_line)
values = tuples.reduceByKey(lambda x, y: x + y)
results = values.collect()

for result in results:
    print(result)