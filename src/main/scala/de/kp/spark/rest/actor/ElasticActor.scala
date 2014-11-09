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
import de.kp.spark.rest.track.{ElasticBuilderFactory => EBF,ElasticContext}

class ElasticActor(ec:ElasticContext) extends Actor with ActorLogging {

  def receive = {
    
    case req:TrackRequest => {
      
      val origin = sender
      origin ! new TrackResponse(ResponseStatus.SUCCESS)

      val topic = req.topic
      topic match {

        /**
         * The amount data structure is based on the RFM model: 
         * R(ecency), F(requency) and M(onetary value) and is an 
         * appropriate starting point for intent recognition
         */
        case "amount" => registerRecord(topic,req.data)
        
        /**
         * The extended item data structure is common to outlier
         * detection
         */
        case "extended_item" => registerRecord(topic,req.data)
        
        /**
         * The item data structure is common to association, series
         * and similarity analysis
         */        
        case "item" => registerRecord(topic,req.data)
        
        /**
         * The decision feature data structure is common to decision
         * analysis
         */        
        case "decision_feature" => registerFeature(topic,req.data)
        
        /**
         * The labeled feature data structure is common to outlier
         * detection and similarity analysis
         */
        case "labeled_feature" => registerFeature(topic,req.data)
        
        /**
         * The targeted feature data structure is common to context-aware
         * analysis
         */
        case "targeted_feature" => registerFeature(topic,req.data)
        
        case _ => {/* do nothing */}

      }
      
    }
    
  }
  
  private def registerRecord(topic:String,params:Map[String,String]) {
   /*
    * Elasticsearch is used as a source and also as a sink; this implies
    * that the respective index and mapping must be distinguished; the source
    * index and mapping used here is the same as for ElasticSource
    */
    val index = params("index")
    val mapping = params("type")
    
    val builder = EBF.getBuilder(topic, mapping)    
    val source = EBF.getSource(topic,params)
 
    ec.register(index,mapping,builder,source)
    
  }
  
  private def registerFeature(topic:String,params:Map[String,String]) {
    
   /*
    * Elasticsearch is used as a source and also as a sink; this implies
    * that the respective index and mapping must be distinguished; the source
    * index and mapping used here is the same as for ElasticSource
    */
    val index = params("index")
    val mapping = params("type")
    
    val (names,types) = EBF.getFields(topic,params)
    
    val builder = EBF.getBuilder(topic, mapping, names, types)    
    val source = EBF.getSource(topic,params)
 
    ec.register(index,mapping,builder,source)
    
  }
  
}