import org.apache.spark.SparkContext
import org.apache.spark.SparkContext
import org.apache.spark.{ Logging, SparkContext, SparkConf }
import com.datastax.spark.connector._, org.apache.spark.SparkContext, org.apache.spark.SparkContext._, org.apache.spark.SparkConf


class OrderSummation {
case class User (user_id: String, first_order_date: String)

case class OrderHistory (user_id: String, city_name: String,   first_name: String, balance: Double,last_name: String,
 phone_number: String, state_name: String, street_address: String,  first_order_date: String , 
zip_code: String)
 
case class UserOrderHistory (user_id: String, balance: Double, first_name: String, last_name: String, city: String, state: String ,first_order_date: String)

val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")
val sc = new SparkContext(conf)

val orders = sc.cassandraTable[OrderHistory]("order_management", "order_history")
val users = sc.cassandraTable[User]("order_management", "users")

val ordersByUserId = orders.keyBy(f =>(f.user_id , f.first_order_date) )
val usersByUserId = users.keyBy(f => (f.user_id , f.first_order_date))

//Join the tables by the user_id
val joinedUsers = ordersByUserId.join(usersByUserId) 

//Create RDD with a the new object type which maps to our new table
val orderHistoryObjects = joinedUsers.map(f => (new UserOrderHistory(f._2._2.user_id , f._2._1.balance   , f._2._1.first_name , f._2._1.last_name, f._2._1.city_name, f._2._1.state_name
    ,f._2._1.first_order_date)))

//get the top ten results 
//val top10 = userAccountObjects.collect.toList.sortBy(_.user_id).reverse.take(10)

val sumOfList = orderHistoryObjects.groupBy ( _.user_id ).mapValues(_.map(_.balance).sum ).collect()

//val sumOfList = userAccountObjects.collect.toList.groupBy ( _.user_id ).aggregate(_.balance)

val newRdd = sc.parallelize(sumOfList)

//save to Cassandra
newRdd.saveToCassandra("order_management", "useraccounts")

}