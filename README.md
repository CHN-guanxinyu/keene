# Keene
Keene is a tiny tool box for scala/spark.

## Core
## Example
For example, the code block shown below:
```scala
object Foo extends App{
  val conf = new SparkConf().setAppName(appName).setMaster(master)
    setxxx.
    setxxx.
    setxxx
    
  val sc = new SparkContext(conf)
  
  sc.xxxx.xxx.collect
  
  val spark = SparkSession.builder.enableHiveSupport.getOrCreate()
  spark.xxx
  
  
  val ssc = new StreamingContext(conf, Seconds(1))
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
We can easily use more chained calls after `import com.keene.core.implicits._`, for example:
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
spark fromKafka KafkaParam("brokers" ,"subscribe") toConsole
```
or
```scala
spark fromKafka("brokers" ,"subscribe") toConsole
```
or
```scala
implicit val param = KafkaParam("brokers", "subscribe", extraOpts=Map("xxx" -> "xxx"))

spark.fromKafka select "xxx" where "xxx" toConsole
```
Correspondingly, you can use `toKafka`:
```scala
df.toKafka("brokers", "topics")
```
or
```scala
implicit val kafkaWriterParam ...
df.toKafka
```
There will be more methods useful such as `toConsole` or `toSomeSource`
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
#### String utils:
```scala
"hi  scala   hello   \n\r   \n  world".replaceBlanks(",") //hi,scala,hello,world
//str.replaceBlanks default str.replaceBlanks(" ")
//str.removeBlanks => str.replaceBlanks("")
```
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
  //show when there is --help
  def usage = "usage message"
}

//java package.to.Foo --foo-bar hello --bar-foo true
object Foo extends App{
  val (myArgs, varMap) = args.as[MyArgs]
  //myArgs.fooBar, myArgs.barFoo
  //varMap is a Map("fooBar"->hello, "barFoo"->true)
}
```


# Xsql
通过xml来构建数据流，业务与框架分离解耦，方便开发
## 快速开始
_**``在`bin/xsql_demo.sh`和`conf/xsql_demo/`给出了一个简单的例子``**_
### 基本元素:`<data-sources>`与`<results>`
* 至少两个文件，两个根节点分别是上面的两个
* results中通过dependency依赖data-sources所在文件
### `<data-sources>`标签
核心元素: `<table>`
* 每个`<table>`得到的是一个spark sql上下文中的表
* 每个`<table>`都需要标明查询哪些表

#### 栗子1:从数据源中读取表
```xml
<?xml version="1.0" encoding="UTF-8" ?>
<data-sources>

    <table name="table_from_hive"
           source="hive"
           sql="select * from ad.ad_base_click where dt = ${date}"           
    />
    
</data-sources>
```
* 这段代码的意思是：运行sql语句，从hive中取，得到的表注册成table_from_hive
* `${date}`的值是脚本中的参数`--date 2018-08-xx`，当然，有其他参数也可以这么取，如果需要全局参数，请参照【自定义Context】一节
* `source`的可选项与对应`<table>`标签需要的属性
    1. source="hive" 查询hive中的表
        * `sql="select a"` 就是普通的查询语句。可带el表达式。
    2. hdfs 读取指定路径中文件生成表
        * `path=""`    必填。文件所在hdfs路径。
        * `format=""`  可选。 [所有sparksql支持的文件格式，默认为orc]
    3. text-file 读取textFile生成的表
        * `path=""`    必填。同上。
        * `schema="col1, col2"`  必填。生成表的schema。
        * `split=","`   可选。文件行分隔符。根据切分生成表的各列。默认`\t`
     
#### 栗子2:从已经生成的表中读取表
```xml
<?xml version="1.0" encoding="UTF-8" ?>
<data-sources>

    <column-group id="group_id_1">
        <column name="col_3"/>
        <column name="col_4"/>
        <column name="col_5"/>
        <column name="col_6"/>
    </column-group>
    
    <table name="table_1"
           from="table_from_hive"
           sql="
            select 
                col_1,
                col_2,
                ${COLUMN_GROUP.group_id_1} 
            from table_from_hive 
            where field = 100"
    />
</data-sources>
```
* 这句话的意思是：执行 `sql`属性的sql语句，他依赖于`table_from_hive`，最后以`table_1`的表名注册到上下文
* 例子中可以通过`${COLUMN_GROUP.group_id}`复用字段，在多个`<table>`都用到这些列的情况下能大大减少代码量
* `group by`子句中也可以用`${COLUMN_GROUP.group_id}`
* `<column-group>`标签组中可以使用`<include group-id="group_id">`进行字段复用

#### 栗子3：union
```xml
<table name="t_x"
       from="t_1, t_2, t_3"
       union-table="temp_union"
       sql="select a, b, c from temp_union where field = ${something}"
/>
```

#### 栗子4：自定义Context
```scala
import com.keene.spark.xsql.base.ExecutionContext
class MyContext extends ExecutionContext{
  override def extEnv = {
    //udf
    spark.udf.register("my_fun", function )
    //udaf
    spark.udf.register("my_agg_fun", new MyUDAF)
    //全局变量，通过el表达式可以取出来
    setVar("MY_GLOBAL_VAR","foo")
  }
}
```
然后去执行脚本里添加参数 `--exe-class "package.to.MyContext"`多个context用逗号隔开，之后就可以去xml里使用了
```xml
<table name="t_x"
       from="t_1"
       sql="select a, b, my_fun(v) from temp_union where field = ${MY_GLOBAL_VAR}"
/>
<table name="t_x"
       from="t_1"
       sql="select a, b, my_agg_fun(v) from temp_union where group by d"
/>
```
#### 栗子5：自定义转换器
* 有些复杂的逻辑使用sql无法实现，需要借助spark代码实现
* 这里通过converter属性指定AbcdConverter全类名,自定义的ABCDConverter继承Converter接口
* 需要实现converter : String => DataFrame函数,传入的就是标签中from的表名(如果知道上下文已经有其他表也可以直接用,不过不推荐,很可能上下文中没有那张表而报错)
* 输出的就是想要的结果

```scala
import com.keene.spark.xsql.base.Converter
import org.apache.spark.sql.DataFrame
class MyConverter extends Converter{
  override def convert (table: String) : DataFrame = {
    s"select xxx from $table where f = ${getVar("key", default = "15")}".go
  }
}
```

然后去xml通过table标签的非source节点，用converter属性指定该类：

```xml
<table name="t_x"
       from="t_1"
       converter="package.to.MyConverter"
/>
```
### `<results>`标签
核心元素: `<result>` `<dependencies>`
* 每个`<result>`标签描述了把哪张表，以什么样的格式，输出到哪里
* 需要将内容写到新的文件中，通过`<dependencies>`依赖`<data-sources>`所在文件


#### 栗子6:将table_1输出到不同的target
```xml
<?xml version="1.0" encoding="UTF-8" ?>
<results>
    <dependencies>
        <dependency file="指向`<data-sources>`所在文件的绝对路径"/>
    </dependencies>

    <result from="table_1"
            to="console"
            show="100"
    />
    
    <result from="table_1"
            to="hdfs"
            path="pathTo"
            repartition-num="可选，default 1000"
            format="可选，所有spark sql支持的文件格式，默认orc"
    />
    
    <result from="table_1"
            to="hive"
            repartition-num="可选，default 1000"
            path="可选，如果需要优化为先存临时路径，在load进hive，需要填此属性"
            format="可选，如果填写temp—path，则可以指定format，默认orc"
            table="写入hive的表名"
            
    />
</results>
```
### 更多
#### 栗子7:`<data-sources>`文件的跨文件依赖
file1.xml:
```xml
<?xml version="1.0" encoding="UTF-8" ?>
<data-sources>

    <column-group id="group_id_1">
        <column name="col_3"/>
        <column name="col_4"/>
        <column name="col_5"/>
        <column name="col_6"/>
    </column-group>
    
    <table name="table_from_hive"
           source="hive"
           sql="
            select *
            from ad.ad_base_click
            where dt = ${date}"           
    />
        
    <table name="table_1"
           from="table_from_hive"
           sql="
            select 
                col_1,
                col_2,
                ${COLUMN_GROUP.group_id_1} 
            from table_from_hive 
            where field = 100"
    />
    
    <table name="table_2"
           from="table_from_hive"
           sql="
            select 
                ${COLUMN_GROUP.group_id_1},
                agg_function(col_1) as foo
            from table_from_hive 
            group by ${COLUMN_GROUP.group_id_1}"
    />
</data-sources>
```
程序1用到的file2.xml
```xml
<?xml version="1.0" encoding="UTF-8" ?>
<data-sources>
    <dependencies>
        <dependency file="absolutePathTo/file1.xml"/>
    </dependencies>
    
    <table name="table_3"
           from="table_2"
           sql="
            select foo, bar
            from table_2"
    />
</data-sources>
```
程序2用到的file3.xml
```xml
<?xml version="1.0" encoding="UTF-8" ?>
<data-sources>
    <dependencies>
        <dependency file="absolutePathTo/file1.xml"/>
    </dependencies>
    
    <table name="table_3"
           from="table_2"
           sql="
            select foo, bar
            from table_2"
    />
    
    <table name="table_4"
           from="table_2, table1"
           sql="
            select foo, bar
            from table_2 a
                left join table_1 b
                on a.id = b.id
            where xxxx"
    />
</data-sources>
```
通过这种方式解决不同计算程序间代码冗余的操作，也可以防止数据不一致性。
### 最后
* 由于每个中间表都是在spark上下文中注册了的，所以在调试的时候可以先取消`<results>`的输出，用`spark-shell --jars xxx.jar`去执行main类，然后就可以随意的分析查看相关数据了。
* 默认情况下，程序会维护一个map，解析xml的时候，对于每个表名都存有依赖他的数量，`from`属性就是用来计算这个数量的，当被依赖数量大于1的时候，默认进行cache。
* 不需要关心表会读多的问题，因为程序会根据xml进行反向解析，从`<results>`的每一项开始向前不断通过`<result>`的`input` 属性和`<table>`的`from`属性寻找依赖表，也就是只会读用到的表。
* 对于不同计算程序合理使用`<dependencies>`会大大减少代码量。
* shell脚本中只需要通过`--config-path`指定`<results>`所在文件的绝对路径即可。
* 框架中会在上下文中注册许多常用的udf/udaf，供开发者使用，后续会给出列表。结合spark sql的内置函数，基本可以满足绝大部分的计算需求。
* 开发者可以继承ExecutionContext接口，实现`override def extEnv()`方法，可以注册自己的udf和udaf，打包后通过脚本的`--ext-class`指定类名，多个的话用逗号隔开，即可在xml中使用相应udf了。
