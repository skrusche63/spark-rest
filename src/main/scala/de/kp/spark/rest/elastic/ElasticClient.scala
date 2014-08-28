package de.kp.spark.rest.elastic
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Spark-REST project
* (https://github.com/skrusche63/spark-rest).
* 
* Spark-REST is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Spark-REST is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Spark-REST. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory

import akka.pattern.ask

import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.duration.Duration._

import scala.concurrent.duration.DurationInt

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class ElasticClient {

  private val name = "elastic-client"
  private val path = "client.conf"
    
  private val conf = ConfigFactory.load(path)
  
  private val url = conf.getConfig("elastic").getString("url")

  private val duration = conf.getConfig("elastic").getInt("timeout")
  implicit val timeout = Timeout(DurationInt(duration).second)
    
  private val system = ActorSystem(name, conf)
  private val remote = system.actorSelection(url)

  def send(req:Any):Future[Any] = ask(remote, req)    
  def shutdown() = system.shutdown

}