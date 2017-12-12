%AddJar https://repo.eclipse.org/content/repositories/paho-releases/org/eclipse/paho/org.eclipse.paho.client.mqttv3/1.0.2/org.eclipse.paho.client.mqttv3-1.0.2.jar -f
%AddJar http://central.maven.org/maven2/com/google/code/gson/gson/2.2.4/gson-2.2.4.jar -f
%AddJar https://github.com/sathipal/spark-streaming-mqtt-with-security_2.10-1.3.0/releases/download/0.0.1/spark-streaming-mqtt-security_2.10-1.3.0-0.0.1.jar -f


import org.eclipse.paho.client.mqttv3._
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.mqtt._
import org.apache.spark.SparkConf

val ssc = new StreamingContext(sc.asInstanceOf[SparkContext], Seconds(1))

val lines = MQTTUtils.createStream(ssc, // Spark Streaming Context
"ssl://3etv6p.messaging.internetofthings.ibmcloud.com:8883", // Watson IoT Platform URL
"iot-2/type/+/id/+/evt/+/fmt/+", // MQTT topic to receive the events
"a:3etv6p:random", // Unique ID of the application
"a-3etv6p-7icb4uf5fh", // API-Key
"wgbXafCw*!eR3x?lDm") // Auth-Token

import java.util.Map.Entry
import com.google.gson.JsonElement
import com.google.gson.JsonParser
import java.util.Set


val deviceMappedLines = lines.map(x => ((x.split(" ", 2)(0)).split("/")(4), x.split(" ", 2)(1)))

// Map the Json payload into scala map
val jsonLines = deviceMappedLines.map(x => {
 var dataMap:scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map()
 val payload = new JsonParser().parse(x._2).getAsJsonObject()
 var deviceObject = payload
 val setObj = deviceObject.entrySet()
 val itr = setObj.iterator()
 while(itr.hasNext()) {
 val entry = itr.next();
 try {
 dataMap.put(entry.getKey(), entry.getValue().getAsDouble())
 } catch {
 case e: Exception => dataMap.put(entry.getKey(), entry.getValue().getAsString())
 }
 }
 (x._1, dataMap)
})

/**
 * Create a simple threshold rule. If cpu usage is greater than 10, alert
 */
 
 val threasholdCrossedLines = jsonLines.filter(
 x => {
 var status = false;
 val lat = x._2.get("latitude")
 val lon = x._2.get("longitude")
 if(lat != null && lon != null) {
	 /* example */
 if(lat.get.asInstanceOf[Double] > 41.90 && lon.get.asInstanceOf[Double] > 12.50) {
 status = true
 }
 }
 status
 })

threasholdCrossedLines.print()

ssc.start()
ssc.awaitTermination()
