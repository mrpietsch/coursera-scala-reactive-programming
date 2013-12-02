package nodescala

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.async.Async.async
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.collection._
import scala.collection.JavaConversions._
import java.util.concurrent.{ThreadPoolExecutor, TimeUnit, LinkedBlockingQueue}
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import java.net.InetSocketAddress
import scala.util.{Failure, Try, Success}

/** Contains utilities common to the NodeScala© framework.
 */
trait NodeScala {
  import NodeScala._

  def port: Int

  def createListener(relativePath: String): Listener

  /** Uses the response object to respond to the write the response back.
   *  The response should be written back in parts, and the method should
   *  occasionally check that server was not stopped, otherwise a very long
   *  response may take very long to finish.
   *
   *  @param exchange     the exchange used to write the response back
   *  @param token        the cancellation token for
   *  @param response     the response to write back
   */
  private def respond(exchange: Exchange, token: CancellationToken, response: Response): Unit = {
    if (token.nonCancelled && response.hasNext) {

      val nextFuture: Future[String] = future {
        response.next()
      }

      val result: Try[String] = Try(Await.result(nextFuture, 2 seconds))

      result match {
        case Success(s) => {
          exchange.write(s)
          respond(exchange, token, response)
        }
        case Failure(t) => exchange.close()
      }
    } else {
      exchange.close()
    }
  }

  /** A server:
   *  1) creates and starts an http listener
   *  2) creates a cancellation token (hint: use one of the `Future` companion methods)
   *  3) as long as the token is not cancelled and there is a request from the http listener
   *     asynchronously process that request using the `respond` method
   *
   *  @param relativePath   a relative path on which to start listening on
   *  @param handler        a function mapping a request to a response
   *  @return               a subscription that can stop the server and all its asynchronous operations *entirely*.
   */
  def start(relativePath: String)(handler: Request => Response): Subscription = {

    val listener: Listener = createListener(relativePath)
    val listenerSubscription: Subscription = listener.start()

    val working: Subscription = Future.run() {
      ct => future {
        while (ct.nonCancelled) {
          val requestFuture: Future[(NodeScala.Request, Exchange)] = listener.nextRequest()
          requestFuture.onSuccess {

            case (r, ex) => {
              val handlerFuture: Future[NodeScala.Response] = future { handler(r) }
              val handlerResult: NodeScala.Response = Await.result(handlerFuture, 1 seconds)
              val responseFuture: Future[Unit] = future { respond(ex, ct, handlerResult) }

              val responseResult: Try[Unit] = Try(Await.result(responseFuture, 3 seconds))

              responseResult match {
                case Failure(t) => {
                  ex.close()
                }
                case _ => // nothing
              }
            }
          }
        }
      }
    }

    Subscription(working, listenerSubscription)
  }

}


object NodeScala {

  /** A request is a multimap of headers, where each header is a key-value pair of strings.
   */
  type Request = Map[String, List[String]]

  /** A response consists of a potentially long string (e.g. a data file).
   *  To be able to process this string in parts, the response is encoded
   *  as an iterator over a subsequences of the response string.
   */
  type Response = Iterator[String]

  /** Used to write the response to the request.
   */
  trait Exchange { 
    /** Writes to the output stream of the exchange.
     */
    def write(s: String): Unit

    /** Communicates that the response has ended and that there
     *  will be no further writes.
     */
    def close(): Unit

    def request: Request

  }

  object Exchange {
    def apply(exchange: HttpExchange) = new Exchange {
      val os = exchange.getResponseBody()
      exchange.sendResponseHeaders(200, 0L)

      def write(s: String) = os.write(s.getBytes)

      def close() = os.close()

      def request: Request = {
        val headers = for ((k, vs) <- exchange.getRequestHeaders) yield (k, vs.toList)
        immutable.Map() ++ headers
      }
    }
  }

  trait Listener {
    def port: Int

    def relativePath: String

    def start(): Subscription

    def createContext(handler: Exchange => Unit): Unit

    def removeContext(): Unit

    /** Given a relative path:
      * 1) constructs an uncompleted promise
      * 2) installs an asynchronous request handler using `createContext`
      * that completes the promise with a request when it arrives
      * and then deregisters itself using `removeContext`
      * 3) returns the future with the request
      *
      * @return the future holding the pair of a request and an exchange object
      */
    def nextRequest(): Future[(Request, Exchange)] = {
      val p = Promise[(NodeScala.Request, Exchange)]()

      createContext(ex => {
        p.complete(Success(ex.request, ex))
        removeContext()
      })

      p.future
    }
  }

  object Listener {
    class Default(val port: Int, val relativePath: String) extends Listener {
      private val s = HttpServer.create(new InetSocketAddress(port), 0)
      private val executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue)
      s.setExecutor(executor)

      def start() = {
        s.start()
        new Subscription {
          def unsubscribe() = {
            s.stop(0)
            executor.shutdown()
          }
        }
      }

      def createContext(handler: Exchange => Unit) = s.createContext(relativePath, new HttpHandler {
        def handle(httpxchg: HttpExchange) = handler(Exchange(httpxchg))
      })

      def removeContext() = s.removeContext(relativePath)
    }
  }

  /** The standard server implementation.
   */
  class Default(val port: Int) extends NodeScala {
    def createListener(relativePath: String) = new Listener.Default(port, relativePath)
  }

}
