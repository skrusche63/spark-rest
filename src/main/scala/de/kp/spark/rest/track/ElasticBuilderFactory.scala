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
import org.elasticsearch.common.xcontent.XContentBuilder
  
object ElasticBuilderFactory {

  /******************************************************************
   *                          COMMON
   *****************************************************************/

  val SITE_FIELD:String = "site"
  val TIMESTAMP_FIELD:String = "timestamp"

  val USER_FIELD:String = "user"

  /******************************************************************
   *                          AMOUNT
   *****************************************************************/

  val AMOUNT_FIELD:String = "amount"

  /******************************************************************
   *                          ITEM
   *****************************************************************/

  val GROUP_FIELD:String = "group"
  val ITEM_FIELD:String  = "item"

  /******************************************************************
   *                          EXTENDED ITEM
   *****************************************************************/
  
  val PRICE_FIELD:String  = "price"

  def getBuilder(builder:String,mapping:String,names:List[String]=List.empty[String],types:List[String]=List.empty[String]):XContentBuilder = {
    
    builder match {
      /*
       * An amount comprises (timestamp, site, user, amount) fields
       * and is a common RFM data structure for intent recognition
       */
      case "amount" => new AmountBuilder().createBuilder(mapping)
      /*
       * An item comprises (timestamp, site, user, group, item) fields
       * and is a common data structure for association, series and 
       * similarity analysis
       */
      case "item" => new ItemBuilder().createBuilder(mapping)
      /*
       * An extended item comprises (timestamp, site, user, item, price) 
       * fields and is a common data structure for outlier detection
       */
      case "extended_item" => new ExtendedItemBuilder().createBuilder(mapping)
      /*
       * A labeled features comprises (timestamp, site) fields, a dynamic
       * number of double fields and one string field (label)
       */
      case "labeled_feature" => new LabeledFeatureBuilder().createBuilder(mapping,names,types)
      /*
       * A targeted features comprises (timestamp, site) fields, a dynamic
       * number of double and one double field (target)
       */
      case "targeted_feature" => new TargetedFeatureBuilder().createBuilder(mapping,names,types)
      /*
       * A decision features comprises (timestamp, site) fields, a dynamic
       * number of double or string fields and one string field (label)
       */
      case "decision_feature" => new DecisionFeatureBuilder().createBuilder(mapping,names,types)
      
      case _ => null
      
    }
  
  }

  def getSource(builder:String,params:Map[String,String]):java.util.Map[String,Object] = {

    builder match {
      
      case "amount" => new AmountBuilder().prepare(params)
      
      case "item" => new ItemBuilder().prepare(params)
      
      case "extended_item" => new ExtendedItemBuilder().prepare(params)
      
      case "labeled_feature" => new LabeledFeatureBuilder().prepare(params)
      
      case "targeted_feature" => new TargetedFeatureBuilder().prepare(params)
      
      case "decision_feature" => new DecisionFeatureBuilder().prepare(params)
      
      case _ => null
      
    }

  }
  
  def getFields(builder:String,params:Map[String,String]):(List[String],List[String]) = {

    builder match {
      
      case "labeled_feature" => new LabeledFeatureBuilder().fieldspec(params)
      
      case "targeted_feature" => new TargetedFeatureBuilder().fieldspec(params)
      
      case "decision_feature" => new DecisionFeatureBuilder().fieldspec(params)
      
      case _ => null
      
    }
    
  }
}