package de.kp.spark.rest.actor
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

import akka.actor.{ActorLogging,Props}
import akka.routing.RoundRobinRouter

import akka.pattern.ask
import akka.util.Timeout

import de.kp.spark.rest.Configuration

import de.kp.spark.rest.track.{ElasticContext,KafkaContext}
import de.kp.spark.rest.model._

import scala.concurrent.duration.DurationInt

class TrackMaster extends MonitoredActor with ActorLogging {

  val (elastic,kafka) = Configuration.tracking
  override val router = if (elastic) {
    
    val ec = new ElasticContext()
    context.actorOf(Props(new ElasticActor(ec)).withRouter(RoundRobinRouter(workers)))
    
  } else {
      /* Load configuration for kafka */
      val brokers = Configuration.kafka
  
      val kafkaConfig = Map("kafka.brokers" -> brokers)
      val kc = new KafkaContext(kafkaConfig)

      context.actorOf(Props(new KafkaActor(kc)).withRouter(RoundRobinRouter(workers)))
    
  }

  def receive = {
    /*
     * Message sent by the scheduler to track the 'heartbeat' of this actor
     */
    case req:AliveMessage => register("TrackMaster")
    
    case req:TrackRequest => {
     
      implicit val timeout:Timeout = DurationInt(time).second
	  	    
	  val origin = sender
      val response = ask(router, req).mapTo[TrackResponse]
      
      response.onSuccess {
        case result => origin ! result
      }
      response.onFailure {
        case result => origin ! new TrackResponse(ResponseStatus.FAILURE)	      
	  }
      
    }
    case _ => {}
    
  }
}