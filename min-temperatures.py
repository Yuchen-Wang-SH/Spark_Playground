from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf=conf)

def parse_line(line):
    l = line.split(',')
    station_id = l[0]
    entry_type = l[2]
    temperature = float(l[3]) / 10
    return (station_id, entry_type, temperature)

lines = sc.textFile("./1800.csv")
parsed = lines.map(parse_line)
filtered_min = parsed.filter(lambda x: "TMIN" in x[1])
id_temp = filtered_min.map(lambda x: (x[0], x[2]))
mins = id_temp.reduceByKey(lambda x, y: min(x, y))
results = mins.collect()

for result in results:
    print(result)