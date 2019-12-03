package sparklyr

import java.util
import java.util.Collections

class WorkerContext(
  sourceArray: Array[org.apache.spark.sql.Row],
  lock: AnyRef,
  closure: Array[Byte],
  columns: Array[String],
  groupBy: Array[String],
  closureRLang: Array[Byte],
  bundlePath: String,
  context: Array[Byte]) {

  import org.apache.spark._
  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql._
  import scala.collection.JavaConversions._

  private var result: Array[Row] = Array[Row]()

  def getClosure(): Array[Byte] = {
    closure
  }

  def getClosureRLang(): Array[Byte] = {
    closureRLang
  }

  def getColumns(): Array[String] = {
    columns
  }

  def getGroupBy(): Array[String] = {
    groupBy
  }

  def getSourceArray(): Array[Row] = {
    sourceArray
  }

  def getSourceArrayLength(): Int = {
    getSourceArray.length
  }

  def getSourceArraySeq(): Array[Seq[Any]] = {
    getSourceArray.map(x => x.toSeq)
  }

  /**
   * Used in the GraalVM mode
   */
  def getSourceArrayArray(): Array[Array[Any]] = {
    getSourceArray.map(x => x.toSeq.toArray)
  }

  def getSourceArrayGroupedSeq(): Array[Array[Array[Any]]] = {
    getSourceArray.map(x => x.toSeq.map(g => g.asInstanceOf[Seq[Any]].toArray).toArray)
  }

  /**
   * Used in the GraalVM mode
   */
  def getSourceArrayGroupedArray(): Array[Array[Array[Array[Any]]]] = {
    getSourceArray.map(x => x.toSeq.map(g => g.asInstanceOf[Seq[Any]].toArray.map(r => r.asInstanceOf[Row].toSeq.toArray)).toArray)
  }

  def setResultArraySeq(resultParam: Any) = {
    if (resultParam.isInstanceOf[java.util.Map[_, _]]) {
      val resultMap = resultParam.asInstanceOf[java.util.Map[_, java.util.Map[_,_]]]
      result = resultMap.values().map(x => Row.fromSeq(x.values().toSeq)).toArray
    } else {
      result = resultParam.asInstanceOf[Array[Any]].map(x => Row.fromSeq(x.asInstanceOf[Array[_]].toSeq))
    }
  }

  /**
   * Used in the GraalVM mode
   */
  def setResultArray(s: java.util.Map[String, Any]): Unit = {
    val nr = s.get("nr").asInstanceOf[Number].intValue()
    if (nr == 0) {
      result = Array[Row]()
      return
    }
    val columnNames = s.get("columns").asInstanceOf[java.util.List[String]]
    val nc = columnNames.size()
    val data: java.util.Map[String, java.util.List[Any]] = if (nr > 1) {
      s.get("data").asInstanceOf[java.util.Map[String, java.util.List[Any]]]
    } else {
      val map = s.get("data").asInstanceOf[java.util.Map[String, Any]]
      val dest = new java.util.HashMap[String, java.util.List[Any]]()
      map.entrySet().foreach(entry => dest.put(entry.getKey, Collections.singletonList(entry.getValue)))
      dest
    }
    val columns = columnNames.map(colName => data.get(colName))
    val factors = s.get("factors") match {
      case factorsMap: java.util.Map[_, _] =>
        columnNames.map(colName => factorsMap.get(colName).asInstanceOf[java.util.List[Any]])
      case factorList: java.util.List[_] =>
        (0 until nc).map(c => {
          val colFact = factorList.get(c)
          if (colFact == null) {
            null
          } else {
            Collections.singletonList(factorList.get(c))
          }
        })
    }

    val res = new Array[Row](nr)
    for (i <- 0 until nr) {
      val row = new Array[Any](nc)
      for (j <- 0 until nc) {
        val column: java.util.List[Any] = columns.get(j)
        val columnFactors: java.util.List[_] = factors.get(j)
        if (columnFactors == null) {
          row(j) = column.get(i)
        } else {
          row(j) = columnFactors.get(column.get(i).asInstanceOf[Number].intValue() - 1) // factors are one-based
        }
      }
      res(i) = Row.fromSeq(row.toSeq)
    }
    result = res
  }

  def getResultArray(): Array[Row] = {
    result
  }

  def finish(): Unit = {
    lock.synchronized {
      lock.notify
    }
  }

  def getBundlePath(): String = {
    bundlePath
  }

  def getContext(): Array[Byte] = {
    context
  }
}
