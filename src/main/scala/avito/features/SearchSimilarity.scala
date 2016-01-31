package avito.features

import com.rockymadden.stringmetric.similarity.DiceSorensenMetric

/**
  * Created by bluebyte60 on 1/31/16.
  */
class SearchSimilarity extends java.io.Serializable {
  var sim = 0d

  def this(a: String, b: String) {
    this()
    parse(a, b)
  }

  def parse(a: String, b: String): Unit = {
    if (a == null || b == null) sim = 0d;
    else sim = DiceSorensenMetric(1).compare(a, b).getOrElse(0d)
  }

}
