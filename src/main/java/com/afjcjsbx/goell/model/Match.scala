package com.afjcjsbx.goell.model

import com.afjcjsbx.goell.model.Match.unknown

object Match {

  val unknown = "unknown"

}

case class Match(
                  fixture: Fixture = Fixture(),
                  league: League = League(),
                  teams: Teams = Teams(),
                  goals: Goals1 = Goals1(),
                  score: Score = Score()/*,
                  events: Seq[Events] = Seq.empty,
                  lineups: Seq[Lineups] = Seq.empty,
                  statistics: Seq[Statistics] = Seq.empty,
                  players: Seq[Players] = Seq.empty
                  */
                )

case class Cards(
                  yellow: Int,
                  red: Int
                )

case class Coach(
                  id: Int,
                  name: String,
                  photo: String
                )

case class Colors(
                   player: Player1,
                   goalkeeper: Player1
                 )

case class Dribbles(
                     attempts: String,
                     success: String,
                     past: String
                   )

case class Duels(
                  total: Int,
                  won: Int
                )

case class Events(
                   time: Time,
                   team: Team,
                   player: Player,
                   assist: Player,
                   `type`: String,
                   detail: String,
                   comments: String
                 )

case class Fixture(
                    id: Int = -1,
                    referee: String = unknown,
                    timezone: String = unknown,
                    date: String = unknown,
                    timestamp: Int = -1,
                    periods: Periods = Periods(),
                    venue: Venue = Venue(),
                    status: Status = Status()
                  )

case class Fouls(
                  drawn: Int,
                  committed: Int
                )

case class Games(
                  minutes: Int,
                  number: Int,
                  position: String,
                  rating: String,
                  captain: Boolean,
                  substitute: Boolean
                )

case class Goals(
                  total: Int,
                  conceded: Int,
                  assists: String,
                  saves: String
                )

case class Home(
                 id: Int = 0,
                 name: String = unknown,
                 logo: String = unknown,
                 winner: Boolean = false
               )

case class League(
                   id: Int = -1,
                   name: String = unknown,
                   country: String = unknown,
                   logo: String = unknown,
                   flag: String = unknown,
                   season: Int = -1,
                   round: String = unknown,
                 )

case class Lineups(
                    team: Team1,
                    coach: Coach,
                    formation: String,
                    startXI: Seq[StartXi],
                    substitutes: Seq[StartXi]
                  )

case class Paging(
                   current: Int,
                   total: Int
                 )

case class Parameters(
                       id: String,
                       timezone: String
                     )

case class Passes(
                   total: Int,
                   key: Int,
                   accuracy: String
                 )

case class Penalty(
                    won: Int,
                    commited: String,
                    scored: Int,
                    missed: Int,
                    saved: String
                  )

case class Periods(
                    first: Int = -1,
                    second: Int = -1
                  )

case class Player(
                   id: Int,
                   name: String
                 )

case class Player1(
                    primary: String,
                    number: String,
                    border: String
                  )

case class Player2(
                    id: Int,
                    name: String,
                    number: Int,
                    pos: String,
                    grid: String
                  )

case class Players(
                    team: Team2,
                    players: Seq[Players1]
                  )

case class Players1(
                     player: Coach,
                     statistics: Seq[Statistics2]
                   )

case class Score(
                  halftime: Goals1 = Goals1(),
                  fulltime: Goals1 = Goals1(),
                  extratime: Goals1 = Goals1(),
                  penalty: Goals1  = Goals1()
                )

case class Shots(
                  total: Int,
                  on: Int
                )

case class StartXi(
                    player: Player2
                  )

case class Statistics(
                       team: Team,
                       statistics: Seq[Statistics1]
                     )

case class Statistics1(
                        `type`: String,
                        value: String
                      )

case class Statistics2(
                        games: Games,
                        offsides: Int,
                        shots: Shots,
                        goals: Goals,
                        passes: Passes,
                        tackles: Tackles,
                        duels: Duels,
                        dribbles: Dribbles,
                        fouls: Fouls,
                        cards: Cards,
                        penalty: Penalty
                      )

case class Status(
                   long: String = unknown,
                   short: String = unknown,
                   elapsed: Int = -1
                 )

case class Tackles(
                    total: String,
                    blocks: String,
                    interceptions: Int
                  )

case class Team(
                 id: Int,
                 name: String,
                 logo: String
               )

case class Team1(
                  id: Int,
                  name: String,
                  logo: String,
                  colors: Colors
                )

case class Team2(
                  id: Int,
                  name: String,
                  logo: String,
                  update: String
                )

case class Teams(
                  home: Home = Home(),
                  away: Home = Home()
                )

case class Goals1(
                        home: Int = 0,
                        away: Int = 0,
                )

case class Time(
                 elapsed: Int,
                 extra: String
               )

case class Venue(
                  id: Int = -1,
                  name: String = unknown,
                  city: String = unknown
                )

