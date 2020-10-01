package org.neutron

import java.util.ArrayList
import java.util.Properties

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.util.serialization.{JSONKeyValueDeserializationSchema, KeyedSerializationSchema, SimpleStringSchema}

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.flink.util.Collector

import com.google.gson.Gson
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode

case class Trade(timestamp: Double, symbol: String, price: Double, size: BigInt, dollar: Double)
case class RiskQuote(symbol: String, vwap: Double, trades: Int, avgTrade: Double, dollar: Double, volume: BigInt, nonjump: Double, jump: Double, endTs: Long)


class RiskProcessFunction
    extends ProcessWindowFunction[Trade, RiskQuote, String, TimeWindow] {
  override def process(key: String,
                       ctx: Context,
                       vals: Iterable[Trade],
                       out: Collector[RiskQuote]
                      ): Unit = {
    val objs = vals.toList
    val sorted = objs.sortBy(_.timestamp)

    val prices = sorted.map(_.price)
    val returns = prices.sliding(2).map { case Seq(x, y, _*) => Math.log(y / x) case _ => 0.0d }.toList

    val rv = returns.map(math.pow(_, 2)).sum[Double]
    val bv = returns.sliding(2).map { case Seq(x, y, _*) => math.abs(y) * math.abs(x) case _ => 0.0d }.toList.sum[Double]
    val _jump = (rv - bv) / rv
    val jump = if (_jump.isNaN) 0.0 else _jump
    val nonjump = 1.0d - jump

    val dollar = sorted.map(_.dollar).sum[Double]
    val volume = vals.map(_.size).sum[BigInt]
    val windowEnd = ctx.window.getEnd
    val vwap = dollar / objs.map(_.size.toDouble).sum[Double]
    val trades = objs.length
    val avgTrade = dollar / trades
    val quote = RiskQuote(key, vwap, trades, avgTrade, dollar, volume, nonjump, jump, windowEnd)
    out.collect(quote)
  }

}


object TradeJob {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val kafkaProperties = new Properties()
    kafkaProperties.setProperty("bootstrap.servers", "kafka:9092")
    kafkaProperties.setProperty("group.id", "flinkstocks")

    val topicProps = new Properties()
    topicProps.setProperty("bootstrap.servers", "kafka:9092")
    topicProps.setProperty("group.id", "flinkstocks")
    topicProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    topicProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val consumer = new KafkaConsumer[String, String](topicProps);
    val symbols: ArrayList[String] = new ArrayList()
    val sym = consumer.listTopics().keySet()
    symbols.addAll(sym)

    print("Subscribing to " + symbols.size().toString + " topics")

    val stockConsumer = new FlinkKafkaConsumer[ObjectNode](symbols,
      new JSONKeyValueDeserializationSchema(true),
      kafkaProperties)

    val riskProducer = new FlinkKafkaProducer[RiskQuote]("quotes",
      new KeyedSerializationSchema[RiskQuote]() {
        override def serializeKey(t: RiskQuote): Array[Byte] = return null

        override def serializeValue(t: RiskQuote): Array[Byte] = {
          val gson = new Gson()
          gson.toJson(t).getBytes()
        }
        override def getTargetTopic(t: RiskQuote): String = "agg-" + t.symbol
      },
      kafkaProperties)


    val kafkaMsgs = env
      .addSource(stockConsumer)
      .map(r => {
        val symbol = r.get("metadata").get("topic").textValue()
        val obj = r.get("value")
        val timestamp = obj.get(0).asDouble
        val price = obj.get(1).asDouble
        val size = obj.get(2).asInt
        val dollar = price * size
        Trade(timestamp, symbol, price, size, dollar)
      })
      .keyBy(_.symbol)

    val riskquotes = kafkaMsgs
      .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
      .process(new RiskProcessFunction)

    riskquotes.addSink(riskProducer)


    env.execute("Flink Trade Processor")
  }
}
