package sparklyr

import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.commons.pool.BasePoolableObjectFactory
import org.apache.commons.pool.impl.GenericObjectPool
import org.graalvm.polyglot.{Context, Engine}

import scala.collection.JavaConversions._

class ContextPool(number: Int) {

  private val size = new AtomicInteger(0)
  private val pool = new ArrayBlockingQueue[GraalVMContext](number)

  def close(): Unit = {
    for (ctx <- pool) {
      ctx.close()
    }
  }

  def fetchContext(): GraalVMContext = {
    pool.poll() match {
      case plugin: GraalVMContext => return plugin
      case null => createOrBlock
    }
  }

  def releasePlugin(ctx: GraalVMContext): Unit = {
    pool.offer(ctx)
  }

  def addPlugin(ctx: GraalVMContext):Unit ={
    pool.add(ctx)
  }

  private def createOrBlock: GraalVMContext = {
    size.get match {
      case e: Int if e == number => block
      case _ => create
    }
  }

  private def create: GraalVMContext = {
    size.incrementAndGet match {
      case e: Int if e > number => size.decrementAndGet; fetchContext()
      case e: Int => new GraalVMContext()
    }
  }

  private def block: GraalVMContext = {
    val timeout = 5000
    pool.poll(timeout, TimeUnit.MILLISECONDS) match {
      case ctx: GraalVMContext => ctx
      case _ => throw new Exception("Couldn't acquire a GraalVM context in %d milliseconds.".format(timeout))
    }
  }
}

object GraalVMContextPool {

  lazy val logger = new Logger("GraalVMContextPool", 0)

//  class ContextFactory() extends BasePoolableObjectFactory {
//    override def makeObject() = new GraalVMContext(debugMode)
//    override def destroyObject(ctx: Object) = ctx.asInstanceOf[GraalVMContext].close()
//  }
  // The lazy field ensures that the pool will not be created on the driver, but on the worker nodes only
  //lazy val pool: GenericObjectPool = new GenericObjectPool(new ContextFactory())
  lazy val pool: ContextPool = new ContextPool(10)

  sys.addShutdownHook {
    pool.close()
  }

  def apply(): GraalVMContext = {
    //pool.borrowObject().asInstanceOf[GraalVMContext]
    pool.fetchContext()
  }

  def release(ctx: GraalVMContext): Unit = {
    //pool.returnObject(ctx)
    pool.releasePlugin(ctx)
  }

}

object GraalVMContext {
  val debugMode = System.getenv("SPARK_FASTR_DEBUG") == "true"

  lazy val engine = {
    var engineBuilder = Engine.newBuilder().allowExperimentalOptions(true)
    if (debugMode) {
      engineBuilder = engineBuilder.option("inspect", "true")
    }
    engineBuilder.build()
  }
}

class GraalVMContext() {
  lazy val (context, closureExecutor) = {
    val ctx = Context.newBuilder().
      engine(GraalVMContext.engine).
      allowAllAccess(true).build()

    val closureExecutor = ctx.eval("R", new Sources().sources)

    GraalVMContextPool.logger.log("GraalVM context ("  + ctx + ") created GGG")

    (ctx, closureExecutor)
  }

  def close(): Unit = {
    GraalVMContextPool.logger.log("GraalVM context ("  + context + ") closed")
    context.close()
  }
}

class WorkerApply(
  closure: Array[Byte],
  columns: Array[String],
  config: String,
  port: Int,
  groupBy: Array[String],
  closureRLang: Array[Byte],
  bundlePath: String,
  customEnv: Map[String, String],
  connectionTimeout: Int,
  context: Array[Byte],
  options: Map[String, String]
  ) {

  import java.io.{File, FileWriter};

  private[this] var exception: Option[Exception] = None
  private[this] var backendPort: Int = 0

  def workerSourceFile(rscript: Rscript): String = {
    val rsources = new Sources()
    val source = rsources.sources

    val tempFile: File = new File(rscript.getScratchDir() + File.separator + "sparkworker.R")
    val outStream: FileWriter = new FileWriter(tempFile)
    outStream.write(source)
    outStream.flush()

    tempFile.getAbsolutePath()
  }

  def apply(iterator: Iterator[org.apache.spark.sql.Row]): Iterator[org.apache.spark.sql.Row] = {
    val sessionId: Int = scala.util.Random.nextInt(10000)
    val logger = new Logger("Worker", sessionId)
    val lock: AnyRef = new Object()

    val data = iterator.toArray;

    // No point in starting up R process to not process anything
    if (data.length == 0) return Array[org.apache.spark.sql.Row]().iterator

    val workerContext = new WorkerContext(
      data,
      lock,
      closure,
      columns,
      groupBy,
      closureRLang,
      bundlePath,
      context
    )

    if (options.getOrElse("graalvm.use-fastr", "") == "true") {

//      GraalVMContextPool.synchronized {
        if (options.contains("rscript.before")) {
          val rscript = new Rscript(logger)
          rscript.run(options.getOrElse("rscript.before", ""))
        }

        val graalContext: GraalVMContext = GraalVMContextPool()
        graalContext.context.enter()
        try {
          logger.log("Applying closure using FastR (" + graalContext + ")")
          graalContext.closureExecutor.execute(sessionId.toString, config, workerContext)
        } finally {
          graalContext.context.leave()
          GraalVMContextPool.release(graalContext)
        }
//      }

    } else {
      val tracker = new JVMObjectTracker()
      val contextId = tracker.put(workerContext)
      logger.log("is tracking worker context under " + contextId)

      logger.log("initializing backend")
      val backend: Backend = new Backend()
      backend.setTracker(tracker)

      /*
       * initialize backend as worker and service, since exceptions and
       * terminating the r session should not shutdown the process
       */
      backend.setType(
        true, /* isService */
        false, /* isRemote */
        true, /* isWorker */
        false /* isBatch */
      )

      backend.setHostContext(
        contextId
      )

      backend.init(
        port,
        sessionId,
        connectionTimeout
      )

      backendPort = backend.getPort()

      new Thread("starting backend thread") {
        override def run(): Unit = {
          try {
            logger.log("starting backend")

            backend.run()
          } catch {
            case e: Exception =>
              logger.logError("failed while running backend: ", e)
              exception = Some(e)
              lock.synchronized {
                lock.notify
              }
          }
        }
      }.start()

      new Thread("starting rscript thread") {
        override def run(): Unit = {
          try {
            logger.log("is starting rscript")

            val rscript = new Rscript(logger)
            val sourceFilePath: String = workerSourceFile(rscript)

            rscript.init(
              List(
                sessionId.toString,
                backendPort.toString,
                config
              ),
              sourceFilePath,
              customEnv,
              options
            )

            lock.synchronized {
              lock.notify
            }
          } catch {
            case e: Exception =>
              logger.logError("failed to run rscript: ", e)
              exception = Some(e)
              lock.synchronized {
                lock.notify
              }
          }
        }
      }.start()

      logger.log("is waiting using lock for RScript to complete")
      lock.synchronized {
        lock.wait()
      }
      logger.log("completed wait using lock for RScript")

      if (exception.isDefined) {
        throw exception.get
      }
    }

    logger.log("is returning RDD iterator with " + workerContext.getResultArray().length + " rows")
    logger.log("Result array:" + workerContext.getResultArray().deep.mkString("\n"))

    return workerContext.getResultArray().iterator
  }
}
