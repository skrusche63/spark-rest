package de.kp.spark.rest.track
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

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

class ItemBuilder {

  import de.kp.spark.rest.track.ElasticBuilderFactory._
  
  def createBuilder(mapping:String):XContentBuilder = {
    /*
     * Define mapping schema for index 'index' and 'type'; note, that we actually support 
     * the following common schema for association, series and similarity analysis: 
     * 
     * timestamp, site, user, group and item.
     */
    val builder = XContentFactory.jsonBuilder()
                      .startObject()
                      .startObject(mapping)
                        .startObject("properties")

                          /* timestamp */
                          .startObject(TIMESTAMP_FIELD)
                            .field("type", "long")
                          .endObject()
                    
                          /* site */
                          .startObject(SITE_FIELD)
                            .field("type", "string")
                            .field("index", "not_analyzed")
                          .endObject()

                          /* user */
                          .startObject(USER_FIELD)
                            .field("type", "string")
                            .field("index", "not_analyzed")
                          .endObject()//

                          /* group */
                          .startObject(GROUP_FIELD)
                            .field("type", "string")
                            .field("index", "not_analyzed")
                          .endObject()//

                          /* item */
                          .startObject(ITEM_FIELD)
                            .field("type", "integer")
                          .endObject()

                        .endObject() // properties
                      .endObject()   // mapping
                    .endObject()
                    
    builder

  }

  def prepare(params:Map[String,String]):java.util.Map[String,Object] = {
    
    val source = HashMap.empty[String,String]
    
    source += SITE_FIELD -> params(SITE_FIELD)
    source += USER_FIELD -> params(USER_FIELD)
      
    source += TIMESTAMP_FIELD -> params(TIMESTAMP_FIELD)
 
    source += GROUP_FIELD -> params(GROUP_FIELD)
    source += ITEM_FIELD -> params(ITEM_FIELD)

    source
    
  }

}