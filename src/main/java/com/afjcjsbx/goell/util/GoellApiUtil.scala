package com.afjcjsbx.goell.util

import com.afjcjsbx.goell.model.Match
import com.afjcjsbx.goell.util.HttpClient.{DEFAULT_TIME_ZONE, NEW_URL_MATCH_BY_ID_V3}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.lang3.text.StrSubstitutor

import java.util
import scala.+:
import scala.collection.mutable.ListBuffer
import scala.io.Source

object GoellApiUtil {

  def getMatchRequest(matchId: Integer): Match = {
    val data = new util.HashMap[String, String]
    data.put("id", String.valueOf(matchId))
    data.put("timezone", DEFAULT_TIME_ZONE)
    val formattedUrl = StrSubstitutor.replace(NEW_URL_MATCH_BY_ID_V3, data)
    val result = HttpClient.sendGet(formattedUrl)
    println(result)

    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)

    val actualObj = objectMapper.readTree(result)
    val jsonNode1 = actualObj.get("response").get(0)

    println(jsonNode1)
    println(jsonNode1.toString)

    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    objectMapper.readValue(jsonNode1.toString, classOf[Match])
  }


  def getMatchesByDate(date: String): Seq[Match] = {
    val fileContents = Source.fromFile("src/main/resources/mockedResponse.json").mkString
    /*
    val data = new util.HashMap[String, String]
    data.put("date", "2021-10-14")
    data.put("timezone", DEFAULT_TIME_ZONE)
    val formattedUrl = StrSubstitutor.replace(NEW_URL_MATCHES_BY_DATE_V3, data)
    val result = HttpClient.sendGet(formattedUrl)
    println(result)

     */

    val matches = new ListBuffer[Match]()


    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    val actualObj = objectMapper.readTree(fileContents)
    val jsonNode1 = actualObj.get("response")
    val iterator = jsonNode1.elements()
    while (iterator.hasNext){
      val m = objectMapper.readValue(iterator.next().toString, classOf[Match])
      matches += m
    }

    println(matches.toSeq)
    /*
    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)

    val actualObj = objectMapper.readTree(result)
    val jsonNode1 = actualObj.get("response").get(0)

    println(jsonNode1)
    println(jsonNode1.toString)

    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    objectMapper.readValue(jsonNode1.toString, classOf[Match])

     */
    Seq.empty
  }


  def main(args: Array[String]): Unit = {
    //println(getMatchRequest(731652))
    println(getMatchesByDate(""))
  }

}
