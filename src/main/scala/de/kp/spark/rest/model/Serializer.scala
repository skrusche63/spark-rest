package de.kp.spark.rest.model

import org.json4s._

import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read,write}

case class ActorInfo(
  name:String,
  timestamp:Long
)

case class ActorStatus(
  name:String,
  date:String,
  status:String
)

case class AliveMessage()

case class ActorsStatus(items:List[ActorStatus])

case class InsightRequest(service:String,data:Map[String,String])

case class InsightResponse(status:String)

case class ServiceRequest(service:String,task:String,data:Map[String,String])

case class ServiceResponse(service:String,task:String,data:Map[String,String],status:String)

case class TrackRequest(topic:String,data:Map[String,String])

case class TrackResponse(status:String)

object Serializer {
    
  implicit val formats = Serialization.formats(NoTypeHints)

  def serializeActorsStatus(stati:ActorsStatus):String = write(stati)
 
  def serializeRequest(request:ServiceRequest):String = write(request)
  def deserializeResponse(response:String):ServiceResponse = read[ServiceResponse](response)

}

object ResponseStatus {
  
  val FAILURE:String = "failure"
  val SUCCESS:String = "success"
  
}