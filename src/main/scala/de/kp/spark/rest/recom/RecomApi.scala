package de.kp.spark.rest.recom
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

import java.util.Date

import akka.actor.{ActorRef,ActorSystem,Props}
import akka.pattern.ask

import akka.util.Timeout

import spray.http.StatusCodes._
import spray.httpx.encoding.Gzip
import spray.httpx.marshalling.Marshaller

import spray.routing.{Directives,HttpService,RequestContext,Route}
import spray.routing.directives.EncodingDirectives
import spray.routing.directives.CachingDirectives

import scala.concurrent.{ExecutionContext}
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt

import scala.util.parsing.json._

import de.kp.spark.rest.{Configuration,RestService}
import de.kp.spark.rest.model._

class RestApi(host:String,port:Int,system:ActorSystem) extends HttpService with Directives {

  val (duration,retries,time) = Configuration.actor   

  implicit val ec:ExecutionContext = system.dispatcher  
  import de.kp.spark.rest.RestJsonSupport._
  
  override def actorRefFactory:ActorSystem = system

  private val RouteCache = CachingDirectives.routeCache(1000,16,Duration.Inf,Duration("30 min"))
  
  val recommender = system.actorOf(Props(new RecomMaster()), name="RecomMaster")
 
  def start() {
    RestService.start(routes,system,host,port)
  }    

  private def routes:Route = {
    /**
     * The 'predict' request supports rating prediction for for a certain provided dataset;
     * the dataset depends on the algorithm selected. These requests are restricted to the 
     * ALS and CAR algorithm.
     */
    path("predict" / Segment) {subject => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doPredict(ctx,subject)
	    }
	  }
    }  ~ 
    path("recommend" / Segment) {subject => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doRecommend(ctx,subject)
	    }
	  }
    }  ~ 
    path("similar" / Segment) {subject =>  
	  post {
	    respondWithStatus(OK) {
	      ctx => doSimilar(ctx,subject)
	    }
	  }
    }  ~ 
    pathPrefix("web") {
      /*
       * 'web' is the prefix for static public content that is
       * served from a web browser and provides a minimalistic
       * web UI for this prediction server
       */
      implicit val actorContext = actorRefFactory
      get {
	    respondWithStatus(OK) {
	      getFromResourceDirectory("public")
	    }
      }
    }
  }
  
  /**
   * 'predict' describes requests to retrieve predictions either 
   * from event or item based models
   */
  private def doPredict[T](ctx:RequestContext,subject:String) = {
    
    val service = "recommendation"
    val task = "predict:" + subject

    val topics = List("event","item")
	if (topics.contains(subject)) doRequest(ctx,service,task)	
    
  }
  /**
   * 'recommend' describes requests to retrieve recommendations 
   * either from event or item based models
   */
  private def doRecommend[T](ctx:RequestContext,subject:String) = {
    
    val service = "recommendation"
    val task = "recommend:" + subject

    val topics = List("item","user")
	if (topics.contains(subject)) doRequest(ctx,service,task)	
    
  }
  /**
   * 'similar' supports the retrieval of items that are similar
   * to those items that have been provided with the request
   */
  private def doSimilar[T](ctx:RequestContext,subject:String) = {

    val service = "recommendation"
    val task = "similar:" + subject

    val topics = List("event")
	if (topics.contains(subject)) doRequest(ctx,service,task)	
    
  }
  
  private def doRequest[T](ctx:RequestContext,service:String,task:String) = {
     
    val request = new ServiceRequest(service,task,getRequest(ctx))
    implicit val timeout:Timeout = DurationInt(time).second
    
    val response = ask(recommender,request).mapTo[ServiceResponse]
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
 
  private def getBodyAsMap(ctx:RequestContext):Map[String,String] = {
   
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
    val body = getBodyAsMap(ctx)
    
    headers ++ body
    
  }

}