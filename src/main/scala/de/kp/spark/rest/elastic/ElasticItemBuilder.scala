package de.kp.spark.rest.elastic
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

import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

class ElasticItemBuilder {
  
  def createBuilder(mapping:String):XContentBuilder = {
    /*
     * Define mapping schema for index 'index' and 'type'; the schema 
     * is used by the association, recommendation, series and other
     * engines
     */
    val builder = XContentFactory.jsonBuilder()
          .startObject()
            .startObject(mapping)
              .startObject("properties")

                /* uid */
                .startObject("uid")
                  .field("type", "string")
                  .field("index", "not_analyzed")
                .endObject()
                    
                /* site */
                .startObject("site")
                   .field("type", "string")
                   .field("index", "not_analyzed")
                .endObject()

                /* user */
                .startObject("user")
                   .field("type", "string")
                   .field("index", "not_analyzed")
                .endObject()//

                /* timestamp */
                .startObject("timestamp")
                  .field("type", "long")
                .endObject()

                /* group */
                .startObject("group")
                   .field("type", "string")
                   .field("index", "not_analyzed")
                .endObject()//

                /* item */
                .startObject("item")
                   .field("type", "integer")
                .endObject()

                /* score */
                .startObject("score")
                   .field("type", "double")
                .endObject()

              .endObject() // properties
            .endObject()   // mapping
          .endObject()
                    
    builder

  }
  
  def createSourceJSON(params:Map[String,String]):List[XContentBuilder] = {

    /*
     * The 'item' field specifies a comma-separated list
     * of item (e.g.) product identifiers. Note, that every
     * item is actually indexed individually. This is due to
     * synergy effects with other data sources
     */
    val items = params("item").split(",")
    /*
     * A trackable event may have a 'score' field assigned;
     * note, that this field is optional
     */
    val scores = if (params.contains("score")) params("score").split(",").map(_.toDouble) else Array.fill[Double](items.length)(0)

    val zipped = items.zip(scores)
    
    /* Common field values */
    val uid = params("uid")

    val site = params("site")
    val user = params("user")
     
    val timestamp = params("timestamp").toLong 
    val group = params("group")

    zipped.map(x => {
      
      val (item,score) = x

      val builder = XContentFactory.jsonBuilder()
	  builder.startObject()
	  
	  builder.field("uid", uid)

	  builder.field("site", site)
	  builder.field("user", user)

	  builder.field("timestamp", timestamp)
	  builder.field("group", group)

	  builder.field("item", item)
	  builder.field("score", score)
	  
      builder.endObject()
	  
      builder
    
    }).toList
    
  }

}