package de.kp.spark.rest
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

import akka.actor.{ActorSystem,Props}
import akka.pattern.ask

import akka.util.Timeout

import spray.http.StatusCodes._
import spray.routing.{Directives,HttpService,RequestContext,Route}

import scala.concurrent.{ExecutionContext}
import scala.concurrent.duration.DurationInt

import scala.util.parsing.json._

import de.kp.spark.rest.actor.{EventMaster,InsightMaster,MiningMaster,SearchMaster}

class RestApi(host:String,port:Int,system:ActorSystem) extends HttpService with Directives {

  implicit val ec:ExecutionContext = system.dispatcher  
  import de.kp.spark.rest.RestJsonSupport._
  
  override def actorRefFactory:ActorSystem = system
  
  /* Event master actor */
  val eventMaster = system.actorOf(Props[EventMaster], name="event-master")
  
  /* Insight master actor */
  val insightMaster = system.actorOf(Props[InsightMaster], name="insight-master")
  
  /* Mining master actor */
  val miningMaster = system.actorOf(Props[MiningMaster], name="mining-master")
  
  /* Search master actor */
  val searchMaster = system.actorOf(Props[SearchMaster], name="search-master")
 
  def start() {
    RestService.start(routes,system,host,port)
  }
  
  private def routes:Route = {

    path("event") {
	  post {
	    respondWithStatus(OK) {
	      ctx => event(ctx)
	    }
	  }
    }  ~ 
    path("insight") {
	  post {
	    respondWithStatus(OK) {
	      ctx => insight(ctx)
	    }
	  }
    }  ~ 
    path("mining") {
	  post {
	    respondWithStatus(OK) {
	      ctx => event(ctx)
	    }
	  }
    }  ~ 
    path("search") {
	  post {
	    respondWithStatus(OK) {
	      ctx => search(ctx)
	    }
	  }
    }

  }
  
  private def event[T](ctx:RequestContext) = {
    
    val req = getRequest(ctx)
    val message = new EventMessage(req)
      
    val duration = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(duration).second
    
    val response = ask(eventMaster,message).mapTo[EventResponse] 
    ctx.complete(response)
    
  }

  private def insight[T](ctx:RequestContext) = {
     
    val req = getRequest(ctx)
    val message = new InsightMessage(req)
      
    val duration = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(duration).second
    
    val response = ask(insightMaster,message).mapTo[InsightResponse] 
    ctx.complete(response)
   
  }
  
  private def mining[T](ctx:RequestContext) = {
     
    val req = getRequest(ctx)
    val message = new MiningMessage(req)
      
    val duration = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(duration).second
    
    val response = ask(miningMaster,message).mapTo[MiningResponse] 
    ctx.complete(response)
    
  }
  
  private def search[T](ctx:RequestContext) = {
     
    val req = getRequest(ctx)
    val message = new SearchMessage(req)
      
    val duration = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(duration).second
    
    val response = ask(searchMaster,message).mapTo[SearchResponse] 
    ctx.complete(response)
   
  }

  private def getHeaders(ctx:RequestContext):Map[String,String] = {
    
    val httpRequest = ctx.request
    
    /* HTTP header to Map[String,String] */
    val httpHeaders = httpRequest.headers
    
    Map() ++ httpHeaders.map(
      header => (header.name,header.value)
    )
    
  }
 
  private def getBody(ctx:RequestContext):Map[String,String] = {
   
    val httpRequest = ctx.request
    val httpEntity  = httpRequest.entity    

    val body = JSON.parseFull(httpEntity.data.asString) match {
      case Some(map) => map
      case None => Map.empty[String,String]
    }
      
    body.asInstanceOf[Map[String,String]]
    
  }
  
  private def getRequest(ctx:RequestContext):Map[String,String] = {

    val headers = getHeaders(ctx)
    val body = getBody(ctx)
    
    headers ++ body
    
  }
}