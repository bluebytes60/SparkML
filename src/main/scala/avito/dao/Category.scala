package avito.dao

import org.apache.spark.rdd.RDD

/**
  * Created by bluebyte60 on 1/29/16.
  */

object Category {

  def parse(data: RDD[String], Type: String): RDD[Category] = {
    val r = data.filter(line => line.split("\t").length >= 4)
      .map(line => new Category(line, Type))
    r
  }

}


class Category extends java.io.Serializable {

  var CategoryID = ""
  var Level = ""
  var ParentCategoryID = ""
  var SubcategoryID = ""
  var CategoryType = ""

  def this(s: String, CategoryType: String) {
    this()
    this.CategoryType = CategoryType
    parse(s)
  }

  def parse(s: String): Unit = {
    val features = s.split("\t")
    CategoryID = features(0)
    Level = features(1)
    ParentCategoryID = features(2)
    SubcategoryID = features(3)
  }

}
