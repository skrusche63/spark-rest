package de.kp.spark.rest.model
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

import org.json4s._

import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read,write}

case class ActorInfo(
  name:String,timestamp:Long
)

case class ActorStatus(
  name:String,date:String,status:String
)

case class AliveMessage()

case class ActorsStatus(items:List[ActorStatus])

/* Request response protocol to interact with remote services */
case class ServiceRequest(service:String,task:String,data:Map[String,String])
case class ServiceResponse(service:String,task:String,data:Map[String,String],status:String)

object Serializer {
    
  implicit val formats = Serialization.formats(NoTypeHints)

  def serializeActorsStatus(stati:ActorsStatus):String = write(stati)
 
  def serializeRequest(request:ServiceRequest):String = write(request)
  def deserializeResponse(response:String):ServiceResponse = read[ServiceResponse](response)

}

object Services {

  val ASSOCIATION:String = "association"
  val CONTEXT:String = "context"

  val DECISION:String = "decision"
  val INTENT:String = "intent"
    
  val OUTLIER:String = "outlier"
  val SERIES:String = "series"
    
  val SIMILARITY:String = "similarity"
  val SOCIAL:String = "social"
    
  val TEXT:String = "text"
    
  private val services = List(ASSOCIATION,CONTEXT,DECISION,INTENT,OUTLIER,SERIES,SIMILARITY,SOCIAL,TEXT)

  def isService(service:String):Boolean = services.contains(service)
  
}

object ResponseStatus {
  
  val FAILURE:String = "failure"
  val SUCCESS:String = "success"
  
}