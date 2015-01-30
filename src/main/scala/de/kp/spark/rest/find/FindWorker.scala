package de.kp.spark.rest.find
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

import de.kp.spark.rest.RemoteContext
import de.kp.spark.rest.actor.BaseActor

import de.kp.spark.rest.model._

class FindWorker(ctx:RemoteContext) extends BaseActor {

  def receive = {

    case req:ServiceRequest => {
	  	    
	  val origin = sender
	  
	  val service = req.service
	  val message = Serializer.serializeRequest(req)
	  
	  val response = ctx.send(service,message).mapTo[String] 
      
      response.onSuccess {
	    
        case result => {
          
          origin ! Serializer.deserializeResponse(result)
          context.stop(self)
        
        }
      }
      response.onFailure {
        case throwable => {
          
          origin ! failure(req,throwable.getMessage)  
          context.stop(self)
          
        }
	  }
      
    }
    
  }

}