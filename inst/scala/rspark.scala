import org.apache.commons.lang.StringEscapeUtils
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.SparkContext

object utils {

  def readColumnInt(rdd: RDD[Row]): Array[Int] = {
    rdd.map(row => row(0).asInstanceOf[Int]).collect()
  }

  def readColumnDouble(rdd: RDD[Row]): Array[Double] = {
    rdd.map(row => row(0).asInstanceOf[Double]).collect()
  }

  def readColumnBoolean(rdd: RDD[Row]): Array[Boolean] = {
    rdd.map(row => row(0).asInstanceOf[Boolean]).collect()
  }

  def readColumnString(rdd: RDD[Row]): String = {
    val column = rdd.map(row => row(0).asInstanceOf[String]).collect()
    val escaped = column.map(string => StringEscapeUtils.escapeCsv(string))
    val joined = escaped.mkString("\n")
    return joined + "\n"
  }

  def readColumnDefault(rdd: RDD[Row]): Array[Any] = {
    rdd.map(row => row(0)).collect()
  }

  def createDataFrame2(sc: SparkContext, cols: Array[Array[_]], partitions: Int): Any = {
    if (cols.length == 0) {
      return(
        sc.parallelize(Array(Row()), 1)
      )
    }

    val columnCount = cols.length

    val firstRow = cols(1).asInstanceOf[Array[_]]
    val rowsCount = firstRow.length

    val rows = List.range(0, rowsCount).map(r => {
      List.range(0, columnCount).map(c => {
        cols(c)(r)
      })
    })

    val data = rows.map(o => {
      Row.fromSeq(o)
    })

    sc.parallelize(data, partitions)
  }

  def createDataFrame(sc: SparkContext, cols: Array[_], partitions: Int): RDD[Row] = {
    if (cols.length == 0) {
      return(
        sc.parallelize(Array(Row()), 1)
      )
    }

    val columnCount = cols.length

    val firstRow = cols(1).asInstanceOf[Array[_]]
    val rowsCount = firstRow.length

    val rows = List.range(0, rowsCount).map(r => {
      List.range(0, columnCount).map(c => {
        val col = cols(c).asInstanceOf[Array[_]]
        col(r)
      })
    })

    val data = rows.map(o => {
      val r = o.asInstanceOf[Array[_]]
      Row.fromSeq(r)
    })

    sc.parallelize(data, partitions)
  }

  def getClass(x: Any): Any = {
    x.getClass.getSimpleName
  }

  def getClasses(x: Array[_]): Array[Any] = {
    x.map(e => e.getClass.getSimpleName)
  }
}
