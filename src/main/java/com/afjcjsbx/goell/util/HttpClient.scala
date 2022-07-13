package com.afjcjsbx.goell.util

import org.apache.commons.lang3.text.StrSubstitutor
import scalaj.http.{Http, HttpOptions}

import java.util

object HttpClient {

  val NEW_URL_MATCH_BY_ID_V3 = "https://v3.football.api-sports.io/fixtures?id=${id}&timezone=${timezone}"
  val NEW_URL_MATCHES_BY_DATE_V3 = "https://v3.football.api-sports.io/fixtures?date=${date}&timezone=${timezone}"
  val DEFAULT_TIME_ZONE = "Europe/Rome"

  def sendGet(url: String): String = {
    val result = Http(url)
      .header("Content-Type", "application/json")
      .header("Charset", "UTF-8")
      .header("x-rapidapi-host", "v3.football.api-sports.io")
      .header("x-rapidapi-key", "9c1af22fb34c535db57973d5e6ecc3c1")
      .option(HttpOptions.readTimeout(10000)).asString
    result.body
  }

}
