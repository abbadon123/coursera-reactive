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

  test("A Future should always be completed") {
    val always = Future.always(517)

    assert(Await.result(always, 0 nanos) == 517)
  }
  test("A Future should never be completed") {
    val never = Future.never[Int]

    try {
      Await.result(never, 1 second)
      assert(false)
    } catch {
      case t: TimeoutException => // ok!
    }
  }

  test("any when f1 is first return f1 value") {
    val any = Future.any(List(
        Future { 1 } ,
        Future.delay(Duration.Inf).andThen { case _ => 2 },
        Future.delay(Duration.Inf).andThen { case _ => throw new Exception }
    ))
    
    assert(Await.result(any, 1 second) == 1)
  }

  test("any when f2 is first return f2 value") {
    val any = Future.any(List(
        Future.delay(Duration.Inf).andThen { case _ => 1 }, 
        Future { 2 } , 
        Future.delay(Duration.Inf).andThen { case _ => throw new Exception }
    ))
    
    assert(Await.result(any, 1 second) == 2)
  }
  
  test("any when f3 is first return f3 failure") {
    val any = Future.any(List(
        Future.delay(Duration.Inf).andThen { case _ => 1 },
        Future.delay(10 nano).andThen { case _ => 2 },
        Future { throw new Exception }
    ))
    
    try {
      Await.result(any, 1 second)
      assert(false)
    } catch {
      case t: Exception => // ok!
    }
  }
  
  test("now") {
    val p = Promise[Int]();
    val f = p.future
    
    // when promise not fulfilled
    try {
      f.now
      assert(false)
    } catch {
      case t: NoSuchElementException => // ok
    }
  
    // when promise fulfilled
    p.success(1)
    assert(f.now == 1)
  }
  
  test("cont with") {
    val f = Future { 1 }.continueWith { f => f.now  + 1 }
    assert(Await.result(f, 1 second) == 2)
  }
  
  test("cont with when f1 is infinitive") {
    val f = Future.delay(Duration.Inf).continueWith { f => assert(false, "This block should never run, because f1 must finish first") }
    
    try {
      Await.result(f, 1 second)
      assert(false)
    } catch {
      case t: TimeoutException => // ok
    }
  }
  
 test("cont") {
    val f = Future { 1 }.continue { r => r.get  + 1 }
    assert(Await.result(f, 1 second) == 2)
  }
  
  test("cont when f1 is infinitive") {
    val f = Future.delay(Duration.Inf).continue { r => assert(false, "This block should never run, because f1 must finish first") }
    
    try {
      Await.result(f, 1 second)
      assert(false)
    } catch {
      case t: TimeoutException => // ok
    }
  }
  
  test("CancellationToken") {
   val p = Promise[Int]()
   
   val working = Future.run() { ct =>
     Future {
        while (ct.nonCancelled) {
//          println("working")
        }
        p.success(1)
      }
    }
   
    //should be yet not finished
   assert(p.isCompleted == false)
   
   working.unsubscribe()
   // shoud be finished
   assert(Await.result(p.future, 1 second) == 1)
   
  }
  
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

}




