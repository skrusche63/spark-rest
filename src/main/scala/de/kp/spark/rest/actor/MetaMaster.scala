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

import akka.pattern.ask
import akka.util.Timeout

import akka.routing.RoundRobinRouter

import de.kp.spark.rest.model._

import scala.concurrent.duration.DurationInt

class MetaMaster extends MonitoredActor with ActorLogging {

  val router = context.actorOf(Props(new MetaActor()).withRouter(RoundRobinRouter(workers)))

  def receive = {
    /*
     * Message sent by the scheduler to track the 'heartbeat' of this actor
     */
    case req:AliveMessage => register("MetaMaster")
    
    case req:ServiceRequest => {

      implicit val timeout:Timeout = DurationInt(time).second
	  	    
	  val origin = sender
      val response = ask(router, req).mapTo[ServiceRequest]
      
      response.onSuccess {
        case result => origin ! result
      }
      response.onFailure {
        case throwable => origin ! failure(req)        
	  }
      
    }
    case _ => {}
    
  }
 
}
