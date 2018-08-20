# Keene
Keene is a tiny tool box for scala/spark.


## Example
For example, the code block shown below:
```scala
object Foo extends App{
  val conf = new SparkConf()
  conf.
  setxxx.
  setxxx.
  setxxx
  val sc = new SparkContext(conf)
  
  sc.xxxx.xxx.collect
  
  val spark = SparkSession.builder.enableHiveSupport.getOrCreate()
  spark.xxx
  
  val ssc = xxx
  
  ssc.xxx
}
```
is reduce to:
```scala
object Bar extends App with SimpleSpark{
  sc.xxxx.xxx.collect
  spark.read.xxxx
  ssc.xxx
}
```
If you want to change spark configuration, you can override the method `sparkConfOpt` :
```scala
def appName = "YourAppName"
def master = "MasterUrl"
def sparkConfOpt = Map(
  "spark.xxx.xxx" -> "true"
)
```


## Implicitors
We can easily use more chained calls, for example:
### SparkSql
```scala
object Foo extends App with SimpleSpark{
  val df = spark sql "select * from talbeName"
  df.xxx
}
``` 
is reduced to:
```scala
object Bar extends App{
  val df = "select * from tableName" go
  df.xxx
}
```
or if you just want to select all from a table, you can:
```scala
object Buz extends App{
  val df = "tableName".tab
  df.xxx
}
```
### Structured Streaming
When we want to fetch data from Kafka:
```scala
spark.
readStream.
format("kafka").
option("foo", "xxx").
option("bar", "xxx").
load
```
is reduced to:
```scala
spark fromKafka KafkaParam("brokers" ,"subscribe")
```
or
```scala
implicit val param = KafkaParam("brokers", "subscribe", extraOpts=Map("xxx" -> "xxx"))
spark.fromKafka select "xxx" where "xxx"
```
Correspondingly, you can use `toKafka`:
```scala
df.toKafka.start
```
### Loading Gzip File
If you want to load data from a gzip file, you can:
```scala
spark gzipDF "path/to/gzipFile"
```
### BroadCast
```scala
val arrBc = Array(1,2,3).bc
rdd map{
  //do something with arrBc
}
```
### Others
#### reflect:
```scala
Class.forName("package.to.ClassA").getConstructor().newInstance().asInstanceOf[ClassA]
```
is reduced to:
```scala
"package.to.ClassA".as[ClassA]
```
#### logger:
```scala
val logger = LoggerFactory getLogger "Console"
logger info "foobar"
```
is reduced to: 
```scala
"foobar".info //or debug/warn/error
```
#### taking sample from collections:
```scala
[seq|list|array|set|map] sample //default 20
[seq|list|array|set|map] sample 100
```
## Arguments Parser
You can easily use the arguments parser:
```scala
import com.keene.core.parsers.Arguments
class MyArgs(var fooBar : String = "", var barFoo : Boolean = false) extends Arguments{
  def usage = "usage message"
}

//java package.to.Foo --foo-bar hello --bar-foo true
object Foo extends App{
  val myArgs = args.as[MyArgs]
  //myArgs.fooBar, myArgs.barFoo
}
```
