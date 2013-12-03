package nodescala



import scala.language.postfixOps
import scala.util.{Try, Success, Failure}
import scala.collection._
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.async.Async.{async, await}
import org.scalatest._
import NodeScala._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NodeScalaSuite extends FunSuite {

  test("A Future should always be created") {
    val always = Future.always(517)

    assert(Await.result(always, 0 nanos) == 517)
  }

  test("Any Future should be returned") {

    val list: List[Future[Int]] = List(Future { 1 }, Future { 2 }, Future { throw new Exception})

    // run this test a couple of times and try to observer all possible return values at least once
    val runs: Set[Try[Int]] = (1 to 10000).foldLeft(Set[Try[Int]]()){
      case (acc, f) =>
        acc + Try(Await.result(Future.any(list), 100 nanos))
    }

    assert(runs.size == list.size)
  }

  test("All Futures should complete successful") {
    val f1 = Future.always(1)
    val f2 = Future.always(2)
    val f3 = Future.always(3)
    val fs = List(f1, f2, f3)
    val all = Future.all(fs)
    val expected = List(1, 2, 3)

    assert(Await.result(all, 1 second) === expected)
  }

  test("Delayed value should not be returned before delay is over") {
    val f1 = Future.delay(1 second)
    try {
      Await.result(f1, 100 millis)
      assert(false)
    } catch {
      case t: TimeoutException => // ok!
    }
  }

  test("Delayed value should be returned after delay is over") {
    val f1 = Future.delay(1 seconds)
    try {
      Await.result(f1, 3 seconds)
    } catch {
      case t: TimeoutException => fail(t.getMessage)
    }
  }

  test("A Future should never be created") {
    val never = Future.never[Int]

    try {
      Await.result(never, 1 second)
      assert(false)
    } catch {
      case t: TimeoutException => // ok!
    }
  }

  test("CancellationTokenSource should allow stopping the computation") {
    val cts = CancellationTokenSource()
    val ct = cts.cancellationToken
    val p = Promise[String]()

    async {
      while (ct.nonCancelled) {
        // do work
      }

      p.success("done")
    }

    cts.unsubscribe()
    assert(Await.result(p.future, 1 second) == "done")
  }

//  test("Future.run") {
//    var sign = false
//
//    val working = Future.run() {
//      ct =>
//        Future {
//          while (ct.nonCancelled) {
//            // doing something
//          }
//          sign = true
//        }
//    }
//    Future.delay(1 seconds) onSuccess {
//      case _ => working.unsubscribe()
//    }
//
//    val asserter = Future.delay(2 seconds)
//    asserter onSuccess {
//      case _ => assert( sign )
//    }
//
//    Await.ready(asserter, 3 seconds)
//  }

  class DummyExchange(val request: Request) extends Exchange {
    @volatile var response = ""
    val loaded = Promise[String]()
    def write(s: String) {
      response += s
    }
    def close() {
      loaded.success(response)
    }
  }

  class DummyListener(val port: Int, val relativePath: String) extends NodeScala.Listener {
    self =>

    @volatile private var started = false
    var handler: Exchange => Unit = null

    def createContext(h: Exchange => Unit) = this.synchronized {
      assert(started, "is server started?")
      handler = h
    }

    def removeContext() = this.synchronized {
      assert(started, "is server started?")
      handler = null
    }

    def start() = self.synchronized {
      started = true
      new Subscription {
        def unsubscribe() = self.synchronized {
          started = false
        }
      }
    }

    def emit(req: Request) = {
      val exchange = new DummyExchange(req)
      if (handler != null) handler(exchange)
      exchange
    }
  }

  class DummyServer(val port: Int) extends NodeScala {
    self =>
    val listeners = mutable.Map[String, DummyListener]()

    def createListener(relativePath: String) = {
      val l = new DummyListener(port, relativePath)
      listeners(relativePath) = l
      l
    }

    def emit(relativePath: String, req: Request) = this.synchronized {
      val l = listeners(relativePath)
      l.emit(req)
    }
  }

  test("Listener should serve the next request as a future") {
    val dummy = new DummyListener(8191, "/test")
    val subscription = dummy.start()

    def test(req: Request) {
      val f = dummy.nextRequest()
      dummy.emit(req)
      val (reqReturned, xchg) = Await.result(f, 1 second)

      assert(reqReturned == req)
    }

    test(immutable.Map("StrangeHeader" -> List("StrangeValue1")))
    test(immutable.Map("StrangeHeader" -> List("StrangeValue2")))

    subscription.unsubscribe()
  }

  test("Server should serve requests") {
    val dummy = new DummyServer(8191)
    val dummySubscription = dummy.start("/testDir") {
      request => for (kv <- request.iterator) yield (kv + "\n").toString
    }

    // wait until server is really installed
    Thread.sleep(500)

    def test(req: Request) {
      val webpage = dummy.emit("/testDir", req)
      val content = Await.result(webpage.loaded.future, 1 second)
      val expected = (for (kv <- req.iterator) yield (kv + "\n").toString).mkString
      assert(content == expected, s"'$content' vs. '$expected'")
    }

    test(immutable.Map("StrangeRequest" -> List("Does it work?")))
    test(immutable.Map("StrangeRequest" -> List("It works!")))
    test(immutable.Map("WorksForThree" -> List("Always works. Trust me.")))

    dummySubscription.unsubscribe()
  }

  test("Server should cancel a long-running or infinite response") {
    def nextStream(lo: Int): Stream[String] = {
      Stream.cons(lo + "", nextStream(lo + 1))
    }
    val dummy = new DummyServer(8191)
    val dummySubscription = dummy.start("/testDir") {
      request => Stream.cons(0 + "", nextStream(0)).iterator
    }

    // wait until server is really installed
    Thread.sleep(500)

    def test(req: Request) {
      val webpage = dummy.emit("/testDir", req)
      val content = Await.result(webpage.loaded.future, 5 second)
    }

    //    val future = Future.delay(2 seconds)
    //    future.onComplete(t => {
    //      dummySubscription.unsubscribe
    //    })

    test(immutable.Map("infinite" -> List("Does it work?")))
  }

}




