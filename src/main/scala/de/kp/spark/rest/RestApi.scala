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

import akka.actor.{ActorRef,ActorSystem,Props}
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
  val trainer = system.actorOf(Props[TrainMaster], name="train-master")

  /*
   * The master actor that handles all (model) retrieval related post requests 
   */
  val finder = system.actorOf(Props[PredictMaster], name="find-master")
  /*
   * The master actor that handles all status related post requests 
   */
  val monitor = system.actorOf(Props[StatusMaster], name="status-master")

  /* Search master actor */
  val searchMaster = system.actorOf(Props[SearchMaster], name="search-master")
 
  def start() {
    RestService.start(routes,system,host,port)
  }
  /*
   * The routes defines the different access channels this API supports
   */
  private def routes:Route = {

    path("admin") {
	  post {
	    respondWithStatus(OK) {
          ctx => admin(ctx)
	    }
	  }
    }  ~ 
    path("event" / Segment) {segment => 
	  post {
	    respondWithStatus(OK) {
	      segment match {
	        case "transaction" => {
	          /*
	           * Request to collect transaction events; this request
	           * is mapped onto the internal 'transaction' topic as
	           * it is collected as contribution to a transaction db
	           */
	          ctx => event(ctx,"transaction")
	        }
	      }
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
    /*
     * Action specifies a concept that is supported by the REST service;
     * other concepts are content,feature,product and state
     */
    path("action/train" / Segment) {segment =>
	  post {
	    respondWithStatus(OK) {
	      ctx => doTrain(ctx,"action",segment)
	    }
	  }
    }  ~ 
    path("action/get" / Segment / Segment) {(service,subject) => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doGet(ctx,"action",service,subject)
	    }
	  }
    }  ~ 
    path("action/status" / Segment) {segment => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doStatus(ctx,"action",segment)
	    }
	  }
    }  ~ 
    /*
     * Content specifies a concept that is supported by the REST service;
     * other concepts are action,feature,product and state
     */
    path("content/train" / Segment) {segment =>
	  post {
	    respondWithStatus(OK) {
	      ctx => doTrain(ctx,"content",segment)
	    }
	  }
    }  ~ 
    path("content/get" / Segment / Segment) {(service,subject) => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doGet(ctx,"content",service,subject)
	    }
	  }
    }  ~ 
    path("content/status" / Segment) {segment => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doStatus(ctx,"content",segment)
	    }
	  }
    }  ~ 
    /*
     * Feature specifies a concept that is supported by the REST service;
     * other concepts are action,content,product and state
     */
    path("feature/train" / Segment) {segment =>
	  post {
	    respondWithStatus(OK) {
	      ctx => doTrain(ctx,"feature",segment)
	    }
	  }
    }  ~ 
    path("feature/get" / Segment / Segment) {(service,subject) =>  
	  post {
	    respondWithStatus(OK) {
	      ctx => doGet(ctx,"feature",service,subject)
	    }
	  }
    }  ~ 
    path("feature/status" / Segment) {segment => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doStatus(ctx,"feature",segment)
	    }
	  }
    }  ~ 
    /*
     * Product specifies a concept that is supported by the REST service;
     * other concepts are action,content,features and state
     */
    path("product/train" / Segment) {segment =>
	  post {
	    respondWithStatus(OK) {
	      ctx => doTrain(ctx,"product",segment)
	    }
	  }
    }  ~ 
    path("product/get" / Segment / Segment) {(service,subject) => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doGet(ctx,"product",service,subject)
	    }
	  }
    }  ~ 
    path("product/status" / Segment) {segment => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doStatus(ctx,"product",segment)
	    }
	  }
    }  ~ 
    /*
     * State specifies a concept that is supported by the REST service;
     * other concepts are action,content,features and product
     */
    path("state/train" / Segment) {segment =>
	  post {
	    respondWithStatus(OK) {
	      ctx => doTrain(ctx,"state",segment)
	    }
	  }
    }  ~ 
    path("state/get" / Segment / Segment) {(service,subject) => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doGet(ctx,"state",service,subject)
	    }
	  }
    }  ~ 
    path("state/status" / Segment) {segment => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doStatus(ctx,"state",segment)
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
  /**
   * Common method to handle all admin requests sent to the REST API
   */
  private def admin[T](ctx:RequestContext) = {
    /*
     * Not implemented yet
     */
  }
  /**
   * Common method to handle all events sent to the REST API
   */
  private def event[T](ctx:RequestContext,topic:String) = {
    
    val request = new EventRequest(topic,getRequest(ctx))
      
    val duration = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(duration).second
    
    val response = ask(eventMaster,request).mapTo[EventResponse] 
    ctx.complete(response)
    
  }

  private def insight[T](ctx:RequestContext,service:String="insight") = {
     
    val request = new InsightRequest(service,getRequest(ctx))
      
    val duration = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(duration).second
    
    val response = ask(insightMaster,request).mapTo[InsightResponse] 
    ctx.complete(response)
   
  }

  private def doGet[T](ctx:RequestContext,concept:String,service:String,subject:String) = {

    service match {
      /*
       * Request to get a decision for a certain feature set; 
       * the request is mapped onto the internal 'decision' service
       * 
       * ../get/decision/feature
       */
      case "decision" => doRequest(ctx,"decision","get:decision")
	  /*
	   * Request to get outliers with respect to features or states;
	   * the request is mapped onto the internal 'outlier' service
	   * 
	   * ../get/outlier/feature|state
	   */
	  case "outlier" => doRequest(ctx,"outlier","get:outlier")

	  case "rule" => {
	    
	    subject match {
	      
	      /*
	       * Request to retrieve the relation models; this request is mapped 
	       * onto the internal 'rule' service
	       * 
	       * ../get/rule/relations
	       */
	      case "relations" => doRequest(ctx,"rule","get:relation")
	      /*
	       * Request to retrieve the rule models; this request is mapped 
	       * onto the internal 'rule' service
	       * 
	       * ../get/rule/rules
	       */
	      case "rules" => doRequest(ctx,"rule","get:rule")
	      
	      case _ => {}
	      
	    }

	  }
	  
	  case "series" => {
	    
	    subject match {
	      /*
	       * Request to get detected patterns from series analysis;
	       * the request is mapped onto the internal 'series' service
	       * 
	       * ../get/series/patterns
	       */
	      case "patterns" => doRequest(ctx,"series","get:pattern")
	      /*
	       * Request to get detected rules from series analysis;
	       * the request is mapped onto the internal 'series' service
	       * 
	       * ../get/series/rules
	       */
	      case "rules" => doRequest(ctx,"series","get:rule")
	      
	      case _ => {}
	      
	    }
	    
	  }

	  case _ => {}
	  
    }
    
  }
  private def doTrain[T](ctx:RequestContext,concept:String,service:String) = {

    service match {
      /*
       * Request to build a decision model with respect to features; 
       * the requestor has to make sure that the appropriate algorithm 
       * is selected, i.e. for features this is RF
       */
      case "decision" => doRequest(ctx,"decision","train")
	  /*
	   * Request to train outlier with respect to features or states; 
	   * the requestor has to make sure that the appropriate algorithm 
	   * is selected, i.e.for features this is KMeans and for states 
	   * this is MARKOV
	   */
	  case "outlier" => doRequest(ctx,"outlier","train")
	  /*
	   * Request to train rule-based models; this request is mapped 
	   * onto the internal 'rule' service and either uses the Top-K or 
	   * Top-K non redundant algorithm 
	   */
	  case "rule" => doRequest(ctx,"rule","train")
	  /*
	   * Request to train series-based models; this request is mapped 
	   * onto the internal 'series' service and either uses the SPADE
	   * or TSR algorithm 
	   */
	  case "series" => doRequest(ctx,"series","train")
      
	  case _ => {}
	  
    }
  
  }

  private def doStatus[T](ctx:RequestContext,concept:String,service:String) = {

    service match {
	  /*
	   * Request to retrieve the status of training (or mining) a decision
	   * model
	   */
	  case "decision" => doRequest(ctx,"decision","status")
	  /*
	   * Request to retrieve the status of training (or mining) an outlier
	   * model
	   */
	  case "outlier" => doRequest(ctx,"outlier","status")
	  /*
	   * Request to retrieve the status of training (or mining) a rule-based
	   * model
	   */
	  case "rule" => doRequest(ctx,"rule","status")
	  /*
	   * Request to retrieve the status of training (or mining) a series-based
	   * model
	   */
	  case "series" => doRequest(ctx,"series","status")
      
	  case _ => {}
	  
    }
   
  }
  
  private def doRequest[T](ctx:RequestContext,service:String,task:String="train") = {
     
    val request = new ServiceRequest(service,task,getRequest(ctx))
      
    val duration = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(duration).second
    
    val response = ask(master(task),request).mapTo[ServiceResponse] 
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
  
  private def master(task:String):ActorRef = {
    
    val req = task.split(":")(0)   
    req match {
      
      case "get"   => finder
      case "train" => trainer
      
      case "status" => monitor
      
      case _ => null
      
    }
  }
}