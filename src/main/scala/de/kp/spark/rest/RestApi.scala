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

import de.kp.spark.rest.actor.{EventMaster,InsightMaster,PredictMaster,SearchMaster,StatusMaster,TrainMaster}

class RestApi(host:String,port:Int,system:ActorSystem) extends HttpService with Directives {

  implicit val ec:ExecutionContext = system.dispatcher  
  import de.kp.spark.rest.RestJsonSupport._
  
  override def actorRefFactory:ActorSystem = system
  
  /* Event master actor */
  val eventMaster = system.actorOf(Props[EventMaster], name="event-master")
  
  /* 
   * The master actor that handles all insight requests 
   */
  val insightMaster = system.actorOf(Props[InsightMaster], name="insight-master")
  
  /* 
   * The master actor that handles all train related post requests 
   */
  val trainMaster = system.actorOf(Props[TrainMaster], name="train-master")

  /*
   * The master actor that handles all prediction related post requests 
   */
  val predictMaster = system.actorOf(Props[PredictMaster], name="predict-master")

  /* Search master actor */
  val searchMaster = system.actorOf(Props[SearchMaster], name="search-master")
  /*
   * The master actor that handles all status related post requests 
   */
  val statusMaster = system.actorOf(Props[StatusMaster], name="status-master")
 
  def start() {
    RestService.start(routes,system,host,port)
  }
  /*
   * The routes defines the different access channels this API supports
   */
  private def routes:Route = {

    path("admin" / Segment) {segment =>
	  post {
	    respondWithStatus(OK) {
	      segment match {
	        case "cross-sell" => {
	          ctx => event(ctx)
	        }
	      }
	    }
	  }
    }  ~ 
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
    path("train" / Segment) {segment =>
	  post {
	    respondWithStatus(OK) {
	      segment match {
	        /*
	         * Request to train cross-sell models; this request is mapped onto
	         * the internal 'arules' service and either uses the Top-K or Top-K
	         * non redundant algorithm 
	         */
	        case "cross-sell" => {
	          ctx => train(ctx,"arules")
	        }
	      }
	    }
	  }
    }  ~ 
    path("predict" / Segment) {segment => 
	  post {
	    respondWithStatus(OK) {
	      segment match {
	        /*
	         * Request to predict cross-sell models; this request is mapped onto
	         * the internal 'arules' service and either uses the Top-K or Top-K
	         * non redundant algorithm 
	         */
	        case "cross-sell" => {
	          ctx => predict(ctx,"arules")
	        }
	      }
	    }
	  }
    }  ~ 
    path("search") {
	  post {
	    respondWithStatus(OK) {
	      ctx => search(ctx)
	    }
	  }
    }  ~ 
    path("status" / Segment) {segment => 
	  post {
	    respondWithStatus(OK) {
	      segment match {
	        /*
	         * Request to retrieve the status of training (or mining) a cross-sell
	         * model; this request is mapped into the internal 'arules' service
	         */
	        case "cross-sell" => {
	          ctx => status(ctx,"arules")
	        }
	      }
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

  private def insight[T](ctx:RequestContext,service:String="insight") = {
     
    val request = new InsightRequest(service,getRequest(ctx))
      
    val duration = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(duration).second
    
    val response = ask(insightMaster,request).mapTo[InsightResponse] 
    ctx.complete(response)
   
  }
 
  /**
   * Common method to handle all predict requests; note, that the
   * route segments are mapped onto a certain service
   */
  private def predict[T](ctx:RequestContext,service:String) = {
     
    val request = new ServiceRequest(service,"predict",getRequest(ctx))
      
    val duration = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(duration).second
    
    val response = ask(predictMaster,request).mapTo[ServiceResponse] 
    ctx.complete(response)
    
  }
  /**
   * Common method to handle all train requests; note, that the
   * route segments are mapped onto a certain service
   */
  private def train[T](ctx:RequestContext,service:String) = {
     
    val request = new ServiceRequest(service,"train",getRequest(ctx))
      
    val duration = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(duration).second
    
    val response = ask(trainMaster,request).mapTo[ServiceResponse] 
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
  /**
   * Common method to handle all status requests; note, that the
   * route segments are mapped onto a certain service
   */
  private def status[T](ctx:RequestContext,service:String) = {
     
    val request = new ServiceRequest(service,"status",getRequest(ctx))
      
    val duration = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(duration).second
    
    val response = ask(statusMaster,request).mapTo[ServiceResponse] 
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