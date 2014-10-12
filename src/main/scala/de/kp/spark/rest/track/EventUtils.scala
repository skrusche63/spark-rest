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

object EventUtils {
  /*
   * Definition of supported event parameters
   */
  val TIMESTAMP_FIELD:String = "timestamp"

  val SITE_FIELD:String = "site"
  val USER_FIELD:String = "user"

  val GROUP_FIELD:String = "group"
  val ITEM_FIELD:String  = "item"

  def prepare(params:Map[String,String]):(String,String,XContentBuilder,java.util.Map[String,Object]) = {

    val index = params("index")
    val mapping = params("type")
    
    val builder = createBuilder(mapping)
    
    val source = HashMap.empty[String,String]
    
    source += SITE_FIELD -> params(SITE_FIELD)
    source += USER_FIELD -> params(USER_FIELD)
      
    source += TIMESTAMP_FIELD -> params(TIMESTAMP_FIELD)
 
    source += GROUP_FIELD -> params(GROUP_FIELD)
    source += ITEM_FIELD -> params(ITEM_FIELD)
    
    (index,mapping,builder,source)
    
  }
  
  private def createBuilder(mapping:String):XContentBuilder = {
    /*
     * Define mapping schema for index 'index' and 'type'; note, that
     * we actually support the following common schema for rule and
     * also series analysis: timestamp, site, user, group and item.
     * 
     * This schema is compliant to the actual transactional as well
     * as sequence source in spark-arules and spark-fsm
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
  
}