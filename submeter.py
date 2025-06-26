import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum

spark = SparkSession.builder.appName("Share").master("local[*]").getOrCreate()
df = spark.read.parquet("data/cleaned_parquet")

tot   = (df.agg((_sum("Global_active_power")/60).alias("kwh")).first()["kwh"])
s1    = (df.agg(_sum("Sub_metering_1").alias("wh")).first()["wh"])/1000
s2    = (df.agg(_sum("Sub_metering_2").alias("wh")).first()["wh"])/1000
s3    = (df.agg(_sum("Sub_metering_3").alias("wh")).first()["wh"])/1000
other = tot - (s1+s2+s3)

labels = ["Kuchnia (SM1)","Pralnia (SM2)","Bojler+AC (SM3)","Pozostałe"]
sizes  = [s1, s2, s3, other]

plt.figure(figsize=(6,6))
plt.pie(sizes, labels=labels, autopct="%1.1f%%", startangle=140)
plt.title("Udział energii według stref (2006-2010)")
plt.tight_layout(); plt.savefig("sub_share_pie.png"); plt.close()
spark.stop()
