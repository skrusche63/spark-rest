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

case class Field(
  name:String,datatype:String,value:String
)
case class Fields(items:List[Field])

/**
 * Param & Params are used to register the model parameters
 * used for a certain data mining or model building task
 */
case class Param(
  name:String,datatype:String,value:String
)
case class Params(items:List[Param])

/* Request response protocol to interact with remote services */
case class ServiceRequest(service:String,task:String,data:Map[String,String])
case class ServiceResponse(service:String,task:String,data:Map[String,String],status:String)

/**
 * Service requests are mapped onto status descriptions 
 * and are stored in a Redis instance
 */
case class Status(
  service:String,task:String,value:String,timestamp:Long
)

case class StatusList(items:List[Status])

object Messages {

  def GENERAL_ERROR(uid:String):String = 
    String.format("""[UID: %s] A general error occurred.""", uid)
  
  def SEARCH_INDEX_CREATED(uid:String):String = 
    String.format("""[UID: %s] Search index created.""", uid)

  def TASK_DOES_NOT_EXIST(uid:String):String = 
    String.format("""[UID: %s] The task does not exist.""", uid)

  def TASK_IS_UNKNOWN(uid:String,task:String):String = 
    String.format("""[UID: %s] The task '%s' is unknown.""", uid, task)
 
  def TRACKED_DATA_RECEIVED(uid:String):String = 
    String.format("""[UID: %s] Tracked data received.""", uid)

}

object Serializer {
    
  implicit val formats = Serialization.formats(NoTypeHints)

  def serializeActorsStatus(stati:ActorsStatus):String = write(stati)
  /*
   * Serialization and de-serialization of field or metadata
   * specification that describe the mapping from external
   * data source fields to internal pre-defined variables
   */
  def serializeFields(fields:Fields):String = write(fields) 
  def deserializeFields(fields:String):Fields = read[Fields](fields)
  /*
   * Serialization and de-serialization of model parameters
   * used to build or train a specific model; these parameters
   * refer to a certain task (uid) and model name (name)
   */
  def serializeParams(params:Params):String = write(params) 
  def deserializeParams(params:String):Params = read[Params](params)

  def deserializeResponse(response:String):ServiceResponse = read[ServiceResponse](response)
  def serializeResponse(response:ServiceResponse):String = write(response)
  
  def deserializeRequest(request:String):ServiceRequest = read[ServiceRequest](request)
  def serializeRequest(request:ServiceRequest):String = write(request)

  def serializeStatus(status:Status):String = write(status)
  def deserializeStatus(status:String):Status = read[Status](status)

  def serializeStatusList(statuses:StatusList):String = write(statuses)
  def deserializeStatusList(statuses:String):StatusList = read[StatusList](statuses)

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
  /**
   * General purpose stati to describe a tracking process
   */    
  val TRACKING_STARTED:String = "predictive-works:tracking:started"
  val TRACKING_FINISHED:String = "predictive-works:tracking:finsihed"
  
}

object Topics {

  val EVENT:String = "event"
  val ITEM:String  = "item"

  val POINT:String = "point"
  
  val RULE:String     = "rule"
  val SEQUENCE:String = "sequence"

  val STATE:String  = "state"
  val VECTOR:String = "vector"
    
  private val topics = List(EVENT,ITEM,POINT,RULE,SEQUENCE,STATE,VECTOR)
  
  def get(topic:String):String = {
    
    if (topics.contains(topic)) return topic    
    throw new Exception("Unknown topic.")
    
  }
  
}