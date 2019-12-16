import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object SparkSQL {
    def main(args:Array[String]) { 
        val sparkConf = new SparkConf().setAppName("HiveFromSpark").setMaster("local")
        val sc = new SparkContext(sparkConf)
        val data=sc.textFile("src/main/homework1.txt")
        val mapr=data.map (p => p.split(' ')(0))
        val ucnt = mapr.map(x => (x,1)).reduceByKey((x,y) => x+y).sortBy(_._2, false)
        val user = ucnt.keys.take(2)
        val mapb=data.filter(p=> p.split(' ')(0)==user(0) || p.split(' ')(0)==user(1)).map(p=>p.split(' ')(3))
        val bcnt= mapb.map(x=>(x,1)).reduceByKey((x,y)=>x+y).sortBy(_._2,false)
        val bus = bcnt.keys.take(2)
        var result1 = bus(0).concat("(").concat(user(0).toString()).concat(",").concat(user(1).toString()).concat(")")
        var result2 = bus(1).concat("(").concat(user(0).toString()).concat(",").concat(user(1).toString()).concat(")")
        println(result1)
        println(result2)
    }
}