// Databricks notebook source
// DBTITLE 1,Task 1
val data1 = sc.textFile("/FileStore/tables/airports.text")

// COMMAND ----------

val rdd = data1.map(line=>line.split(","))

// COMMAND ----------

val res2 = rdd.filter(line=>(line(3).contains("Islands") || line(6)>"40.0") )
res2.take(2)

// COMMAND ----------

val regex = "%Islands%".r

// COMMAND ----------

rdd.filter(x => (regex.pattern.matcher(x(3)).matches)).collect()

// COMMAND ----------

// DBTITLE 1,task 2 
val data1 = sc.textFile("/FileStore/tables/airports.text")
data1.take(3)

// COMMAND ----------

val filtr = data1.filter(x=>(x(8)%2==0))

// COMMAND ----------

filtr.take(3)

// COMMAND ----------

val timestamp = filtr.map(_.split(",")(11))

// COMMAND ----------

timestamp.take(5)

// COMMAND ----------

val count = timestamp.countByValue()
count.take(2)

// COMMAND ----------

// DBTITLE 1,day 2 - saturday (Intersection)
val july = sc.textFile("/FileStore/tables/nasa_july.tsv").filter(x=> !(x.contains("host")))
val aug = sc.textFile("/FileStore/tables/nasa_august.tsv")filter(x=> !(x.contains("host")))


// COMMAND ----------

val july1 = july.map(x=>(x.split("\t")(0),x.split("\t")(2),x.split("\t")(4),x.split("\t")(6)))
val aug1 = aug.map(x=>(x.split("\t")(0),x.split("\t")(2),x.split("\t")(4),x.split("\t")(6)))

// COMMAND ----------

july1.intersection(aug1).count()

// COMMAND ----------

// DBTITLE 1,Intersection of host column
val julHost = july.map(x=>x.split("\t")(0)).filter(x=> x!= "host")
val augHost = aug.map(x=>x.split("\t")(0)).filter(x=> x!= "host")

// COMMAND ----------

val intersection = julHost.intersection(augHost)
intersection.count()

// COMMAND ----------

// DBTITLE 1,day 3 task 1 - Prime number
// /FileStore/tables/numberData.csv

val number = sc.textFile("/FileStore/tables/numberData.csv")

number.take(2)

// COMMAND ----------

def prime(n:Int):Boolean = ! ((2 until n-1) exists (n % _ == 0))

// COMMAND ----------

val header = number.first
number.filter(p => p != header).map(x => x.toInt).filter(x=>prime(x)).reduce((a,b)=>a+b)

// COMMAND ----------

// DBTITLE 1,task2 - Airport Time Lower Case
val data = sc.textFile("/FileStore/tables/airports.text")

// COMMAND ----------

data.map(x=>(x.split(",")(1),x.split(",")(11))).mapValues(x=>x.toLowerCase).take(5)

// COMMAND ----------

// DBTITLE 1,task 3 - total Friends age wise & max per age
val friends = sc.textFile("/FileStore/tables/FriendsData.csv")
friends.take(5)

// COMMAND ----------

val friends_count = friends.filter(line=> !(line.contains("Age"))).map(x=> ( x.split(",")(2), (1,x.split(",")(3).toLong) )).reduceByKey((a,b)=> (a._1+b._1, a._2+b._2)).mapValues(data=> (data._2/data._1) )

// COMMAND ----------

for( (x, y) <- friends_count.collect()  )  println(x + " : " + y) 

// COMMAND ----------

// DBTITLE 1,max friend per age
val friends_max = friends.filter(line=> !(line.contains("Age"))).map(x=> ( x.split(",")(2), x.split(",")(3).toLong )).reduceByKey(math.max(_, _)).sortByKey()

// COMMAND ----------

for((a,b) <- friends_max.collect) println(a +"--" + b)

// COMMAND ----------


