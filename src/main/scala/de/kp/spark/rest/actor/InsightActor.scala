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

import akka.actor.{Actor,ActorLogging}

import de.kp.spark.rest.{InsightMessage,InsightResponse,ResponseStatus}
import de.kp.spark.rest.insight.InsightContext

class InsightActor(ic:InsightContext) extends Actor with ActorLogging {

  implicit val ec = context.dispatcher

  def receive = {
    
    case req:InsightMessage => {
      
      val origin = sender
      val response = ic.send(req).mapTo[InsightResponse]
      
      response.onSuccess {
        case result => origin ! result
      }
      response.onFailure {
        case result => origin ! new InsightResponse(ResponseStatus.FAILURE)	      
	  }
      
    }
    
  }
}