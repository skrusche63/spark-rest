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

import de.kp.spark.rest.model._
import de.kp.spark.rest.track.{ElasticContext,EventUtils}

class ElasticActor(ec:ElasticContext) extends Actor with ActorLogging {

  def receive = {
    
    case req:TrackRequest => {
      
      val origin = sender
      origin ! new TrackResponse(ResponseStatus.SUCCESS)

      val topic = req.topic
      topic match {
        
        case "event" => registerEvent(req.data)
        
        case "feature" => registerFeature(req.data)

      }
      
    }
    
  }
  
  private def registerEvent(params:Map[String,String]) {
    
    val(index,mapping,builder,source) = EventUtils.prepare(params)
    ec.register(index,mapping,builder,source)
    
  }
  
  private def registerFeature(params:Map[String,String]) {
    
  }
}