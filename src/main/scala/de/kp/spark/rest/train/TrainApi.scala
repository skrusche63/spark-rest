package de.kp.spark.rest.train
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

class TrainApi(host:String,port:Int,system:ActorSystem) extends HttpService with Directives {

  val (duration,retries,time) = Configuration.actor   

  implicit val ec:ExecutionContext = system.dispatcher  
  import de.kp.spark.rest.RestJsonSupport._
  
  override def actorRefFactory:ActorSystem = system

  private val RouteCache = CachingDirectives.routeCache(1000,16,Duration.Inf,Duration("30 min"))
  
  val trainer = system.actorOf(Props(new TrainMaster()), name="TrainMaster")
 
  def start() {
    RestService.start(routes,system,host,port)
  }

  /*
   * The routes defines the different access channels this API supports
   */
  private def routes:Route = {
   /*
    * This request describes the initial step of creating a recommender model;
    * the subsequent step (not invoked by the REST API) comprises training 
    * with previously prepared data. Training is initiated through the Akka
    * remote service that interacts with the user preference service
    */
    path("build" / Segment / Segment) {(service,subject) =>
	  post {
	    respondWithStatus(OK) {
	      ctx => doBuild(ctx,service,subject)
	    }
	  }
    }  ~ 
    /*
     * This request provides trackable information either as an event 
     * or as a feature; an event refers to a certain 'item', e.g. an 
     * ecommerce product or service , and a feature refers to a specific
     * dataset
     */
    path("train" / Segment / Segment) {(service,subject) =>
	  post {
	    respondWithStatus(OK) {
	      ctx => doTrain(ctx,service,subject)
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
  
  private def doBuild[T](ctx:RequestContext,service:String,subject:String) = {
 
    val task = "build" +":" + subject
    service match {
    
      case "rating" => {

        val topics = List("event","item")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
        
      }
      case _ => {}
    
    }
    
  } 
  private def doTrain[T](ctx:RequestContext,service:String,subject:String) = {

    val task = "train" +":" + subject
    service match {
      /*
       * Request parameters for the 'train' request
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * - algorithm (String, TOPK, TOPKNR)
       * - source (String, ELASTIC, FILE, JDBC, PARQUET, PIWIK)
       * - sink (String, ELASTIC, JDBC)
       * 
       * and the following parameters depend on the selected source:
       * 
       * ELASTIC:
       * 
       * - source.index (String)
       * - source.type (String)
       * - query (String)
       * 
       * JDBC:
       * 
       * - query (String)
       * 
       * and the following parameters depend on the selected sink:
       * 
       * ELASTIC:
       * 
       * - sink.index (String)
       * - sink.type (String)
       * 
       * and the model building parameters have to be distinguished by the
       * selected algorithm
       * 
       * TOPK
       * 
       * - k (Integer)
       * - minconf (Double)
       * 
       * TOPKNR
       * 
       * - k (Integer)
       * - minconf (Double)
       * - delta (Int)
       * 
       */
	  case "association" => {
	    
	    val topics = List("model")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	

	  }
      /*
       * 'train' requests initiate model building; this comprises 
       * either a correlation matrix on top of a factorization model 
       * or a factorization model itself.
       * 
       * 
       * 'matrix': This request trains a correlation matrix on 
       * top of a factorization model; this matrix can be used 
       * to compute similarities between certain specific features
       * 
       * 'model': This request trains a factorization model from a
       * feature-based dataset and saves this model in a pre-configured 
       * directory on the file system
       * 
       *
       * Request parameters for the 'train' request
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * - source (String, ELASTIC, FILE, JDBC, PARQUET)
       * 
       * and the following parameters depend on the selected source:
       * 
       * ELASTIC:
       * 
       * - source.index (String)
       * - source.type (String)
       * - query (String)
       * 
       * JDBC:
       * 
       * - query (String)
       * 
       * and the matrix building parameters are
       * 
       * - columns (String, comma-separated list of Integers)
       * 
       * or 
       * 
       * - start (Integer)
       * - end (Integer)
       * 
       * and the model building parameters are
       * 
       * - num_attribute (Integer)
       * - num_factor (Integer)
       * 
       * - num_iter (Integer)
       * - learn_rate (Double)
       * 
       * - init_mean (Double)
       * - init_stdev (Double)
       * 
       * - k0 (Boolean)
       * - k1 (Boolean)
       * 
       * - reg_c (Double)
       * - reg_v (Double)
       * - reg_m (Double)
       * 
       */
	  case "context" => {
	    
	    val topics = List("matrix","model")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }
      /*
       * Request parameters for the 'train' request
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * - source (String, ELASTIC, FILE, JDBC)
       * 
       * and the following parameters depend on the selected source:
       * 
       * ELASTIC:
       * 
       * - source.index (String)
       * - source.type (String)
       * - query (String)
       * 
       * JDBC:
       * 
       * - query (String)
       * 
       * and the model building parameters are
       * 
       * - num_selected_features (Integer)
       * - trees_per_node (Integer)
       * - miss_valu (String
       * 
       */
      case "decision" => {
	    
        val topics = List("model")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
      
      }
      /*
       * Request parameters for the 'train' request
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * - algorithm (String, MARKOV, HIDDEN_MARKOV)
       * - source (String, ELASTIC, FILE, JDBC, PIWIK)
       * 
       * - intent (String, STATE)
       * 
       * and the following parameters depend on the selected source:
       * 
       * ELASTIC:
       * 
       * - source.index (String)
       * - source.type (String)
       * - query (String)
       * 
       * JDBC:
       * 
       * - query (String)
       * 
       * and the following parameters depend on the selected sink:
       * 
       * ELASTIC:
       * 
       * - sink.index (String)
       * - sink.type (String)
       * 
       * and the model building parameters have to be distinguished by the
       * selected algorithm
       * 
       * MARKOV
       * 
       * HIDDEN_MARKOV
       * 
       * - epsilon (Double)
       * - iterations (Int)
       *  
       */    
      case "intent" => {
	    
        val topics = List("model")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
      
      }
      /*
       * Request parameters for the 'train' request
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * - algorithm (String, KMEANS, MARKOV)
       * - source (String, ELASTIC, FILE, JDBC, PARQUET)
       * 
       * and the following parameters depend on the selected source:
       * 
       * ELASTIC:
       * 
       * - source.index (String)
       * - source.type (String)
       * - query (String)
       * 
       * JDBC:
       * 
       * - query (String)
       * 
       * and the model building parameters have to be distinguished by the
       * selected algorithm
       * 
       * KMEANS:
       * 
       * - top (Integer)
       * - strategy (String, distance, entropy)
       * 
       * MARKOV:
       *  
       * - strategy (String, missprob, missrate, entreduc)
       * - threshold (Double)
       * 
       */
	  case "outlier" => {
	    
	    val topics = List("model")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }
      /*
       * Request parameters for the 'train' request
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * - algorithm (String, TOPK, TOPKNR)
       * - source (String, ELASTIC, FILE, JDBC, PARQUET, PIWIK)
       * - sink (String, ELASTIC, JDBC)
       * 
       * and the following parameters depend on the selected source:
       * 
       * ELASTIC:
       * 
       * - source.index (String)
       * - source.type (String)
       * - query (String)
       * 
       * JDBC:
       * 
       * - query (String)
       * 
       * and the following parameters depend on the selected sink:
       * 
       * ELASTIC:
       * 
       * - sink.index (String)
       * - sink.type (String)
       * 
       * and the model building parameters have to be distinguished by the
       * selected algorithm
       * 
       * SPADE
       * 
       * - support (Double)
       * 
       * TSR
       * 
       * - k (Integer)
       * - minconf (Double)
       * 
       */
	  case "series" => {
	    
	    val topics = List("model")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }
      /*
       * Request parameters for the 'train' request
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * - algorithm (String, KMEANS, SKMEANS)
       * - source (String, ELASTIC, FILE, JDBC)
       * 
       * and the following parameters depend on the selected source:
       * 
       * ELASTIC:
       * 
       * - source.index (String)
       * - source.type (String)
       * - query (String)
       * 
       * JDBC:
       * 
       * - query (String)
       * 
       * and the model building parameters have to be distinguished by the
       * selected algorithm
       * 
       * KMEANS:
       * 
       * - top (Integer)
       * - iterations (Integer)
       * - strategy (String, distance, entropy)
       * 
       * SKMEANS:
       *  
       * - k (Integer)
       * - top (Integer)
       * - interations (Integer)
       * 
       */
	  case "similarity" => {
	    
	    val topics = List("model")
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
	      case "concept" => doRequest(ctx,"text","train:concept")	
	      
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
  
  private def master(task:String):ActorRef = trainer

}