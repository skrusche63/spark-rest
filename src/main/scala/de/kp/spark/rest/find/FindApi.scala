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

class FindApi(host:String,port:Int,system:ActorSystem) extends HttpService with Directives {

  val (duration,retries,time) = Configuration.actor   

  implicit val ec:ExecutionContext = system.dispatcher  
  import de.kp.spark.rest.RestJsonSupport._
  
  override def actorRefFactory:ActorSystem = system

  private val RouteCache = CachingDirectives.routeCache(1000,16,Duration.Inf,Duration("30 min"))
  
  val finder = system.actorOf(Props(new FindMaster()), name="FindMaster")
 
  def start() {
    RestService.start(routes,system,host,port)
  }

  /*
   * The routes defines the different access channels this API supports
   */
  private def routes:Route = {
     path("find" / Segment / Segment) {(service,subject) => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doFind(ctx,service,subject)
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

  private def doFind[T](ctx:RequestContext,service:String,subject:String) = {

    val task = "get" +":" + subject
    service match {
      /*
       * Request parameters for the 'get' request:
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * Additional parameters for certain topics:
       * 
       * topic: antecedent, consequent
       * - items (String, comma-separated list of Integers)
       * 
       */  
	  case "association" => {
	    
        val topics = List("antecedent","consequent","crule","rule")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	

	  }
	  case "context" => {
	    
	    subject match {	      
          /*
           * 'predict' requests refer to the retrieval of a predicted target
           * variable for a certain feature vector; the respective vector must
           * have the same format as the vectors used in the training dataset.
           * 
           * Request parameters for the 'predict' request:
           * 
           * - site (String)
           * - uid (String)
           * - name (String)
           * 
           * - features (String, comma-separated list of Doubles)
           * 
           */
	      case "predict" => doRequest(ctx,service,"predict:vector")
          /*
           * 'similar' requests refer to the retrieval of features that are
           * most similar to a set of provided features; note, that these
           * features must be subset of those, that have been used to build
           * the correlation matrix.
           * 
           * Request parameters for the 'similar' request:
           * 
           * - site (String)
           * - uid (String)
           * - name (String)
           * 
           * - total (String)
           * - columns (String, comma-separated list of Integers)
           *   OR 
           * 
           * - start (Integer)
           * - end (Integer)
           * 
           */
	      case "similar" => doRequest(ctx,service,"similar:vector")
	      
	      case _ => {}
	      
	    }
	    
	  }
      /*
       * Request parameters for the 'get' request:
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * - features (String, comma separated list of feature values)
       * 
       */  
      case "decision" => {
	    
        val topics = List("feature")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
      
      }
      /*
       * Request parameters for the 'get' request:
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * and the following parameters depend on the selected topic:
       * 
       * topic: observation
       * 
       * This topic supports the retrieval of a sequence of states
       * from a sequence of observations
       * 
       * - observation (String, comma-separated list of Strings)
       *   OR
       * - observations (String, semicolon-separated, comma-separated list of Strings)  
       * 
       * topic: state
       * 
       * This topic supports the retrieval of a sequence of subsequent
       * states that follow a certain 'state'; the request requires the
       * number of 'steps' to look ahead and the name of the reference
       * state
       * 
       * - steps (Int)
       * - state (String) 
       *   OR 
       * - states (String, comma separated list of feature values)
       * 
       */    
      case "intent" => {
	    
        val topics = List("observation","state")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
      
      }
      /*
       * Request parameters for the 'get' request:
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       */  
	  case "outlier" => {
	    
	    val topics = List("product","vector")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }
      /*
       * Request parameters for the 'get' request:
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * Additional parameters for certain topics:
       * 
       * topic: antecedent, consequent
       * - items (String, comma-separated list of Integers)
       * 
       */  
	  case "series" => {
	    
	    val topics = List("antecedent","consequent","pattern","rule")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }
      /*
       * Request parameters for the 'get' request:
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       */  
	  case "similarity" => {
	    
	    val topics = List("sequence","vector")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }
	  case "social" => {
	    
	    subject match {
	      /* Not implemented yet */
	      case _ => {}
	      
	    }
	    
	  }
	  case "text" => {
	    
	    subject match {
	      /* ../get/text/concept */
	      case "concept" => doRequest(ctx,"text","get:concept")	
	      
	      case _ => {}
	      
	    }
	    
	  }

	  case _ => {}
	  
    }
    
  }
  
  private def doRequest[T](ctx:RequestContext,service:String,task:String="train") = {
     
    val request = new ServiceRequest(service,task,getRequest(ctx))
    implicit val timeout:Timeout = DurationInt(time).second
    
    val response = ask(master(task),request).mapTo[ServiceResponse] 
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
  
  private def master(task:String):ActorRef = finder
  
}