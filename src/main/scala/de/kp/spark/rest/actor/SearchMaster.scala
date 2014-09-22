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

import de.kp.spark.rest.{Configuration,SearchMessage,SearchResponse,ResponseStatus}
import de.kp.spark.rest.context.SearchContext

import scala.concurrent.duration.DurationInt

class SearchMaster extends Actor with ActorLogging {
  
  /* Load configuration for routers */
  val (time,retries,workers) = Configuration.router   
  
  val sc = new SearchContext()
  
  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries=retries,withinTimeRange = DurationInt(time).minutes) {
    case _ : Exception => SupervisorStrategy.Restart
  }

  val searchRouter = context.actorOf(Props(new SearchActor(sc)).withRouter(RoundRobinRouter(workers)))

  def receive = {
    
    case req:SearchMessage => {
      
      implicit val ec = context.dispatcher

      val duration = Configuration.actor      
      implicit val timeout:Timeout = DurationInt(duration).second
	  	    
	  val origin = sender
      val response = ask(searchRouter, req).mapTo[SearchResponse]
      
      response.onSuccess {
        case result => origin ! result
      }
      response.onFailure {
        case result => origin ! new SearchResponse(ResponseStatus.FAILURE)	      
	  }
      
    }
  
    case _ => {}
    
  }
  
}