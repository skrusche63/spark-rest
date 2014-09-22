package de.kp.spark.rest
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

/*
 * Event support: Data ingestion from dynamic customer or visitor
 * engagement. Any tracking message that provides a JSON data format 
 * is capable to be sent to the event
 */
case class EventMessage(data:Map[String,String])

case class EventResponse(status:String)

/*
 * Insight support: A common interface to send SQL-alike
 * query statements to get more insight from ingested data
 */
case class InsightMessage(data:Map[String,String])

case class InsightResponse(status:String)

case class TrainRequest(service:String,data:Map[String,String])
case class TrainResponse(status:String)

case class PredictRequest(service:String,data:Map[String,String])
case class PredictResponse(status:String)

/*
 * Search support
 */
case class SearchMessage(data:Map[String,String])

case class SearchResponse(status:String)

object ResponseStatus {
  
  val FAILURE = "failure"
  val SUCCESS = "success"
  
}