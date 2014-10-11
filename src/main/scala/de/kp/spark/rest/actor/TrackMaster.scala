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

import akka.actor.{Actor,ActorLogging,ActorRef,Props}

import akka.pattern.ask
import akka.util.Timeout

import akka.actor.{OneForOneStrategy, SupervisorStrategy}
import akka.routing.RoundRobinRouter

import de.kp.spark.rest.{Configuration,TrackRequest,TrackResponse,ResponseStatus}
import de.kp.spark.rest.track.{ElasticContext,KafkaContext}

import scala.concurrent.duration.DurationInt

class TrackMaster extends Actor with ActorLogging {
  
  /* Load configuration for routers */
  val (time,retries,workers) = Configuration.router   
  
  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries=retries,withinTimeRange = DurationInt(time).minutes) {
    case _ : Exception => SupervisorStrategy.Restart
  }

  val (elastic,kafka) = Configuration.tracking
  val router = if (elastic) {
    
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
    
    case req:TrackRequest => {
      
      implicit val ec = context.dispatcher

      val duration = Configuration.actor      
      implicit val timeout:Timeout = DurationInt(duration).second
	  	    
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