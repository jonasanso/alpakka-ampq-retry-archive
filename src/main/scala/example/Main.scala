package example

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.amqp._
import akka.stream.alpakka.amqp.scaladsl._
import akka.stream.scaladsl.Flow
import retry._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util._

object Main extends App {
  private implicit lazy val actor: ActorSystem = ActorSystem()
  private implicit lazy val materializer: ActorMaterializer = ActorMaterializer()

  val connectionSettings = AmqpConnectionUri("amqp://guess:guess@0.0.0.0:5672/")

  private val retry = new Retry(200.milli, 1.second, 10.second)
  private val queue = RetryQueue("example")

  private val sourceSettings =
    NamedQueueSourceSettings(connectionSettings, queue.name)
      .withDeclarations(queue.declarations: _*)

  val flow: Flow[CommittableIncomingMessage, Outcome, NotUsed] =
    Flow[CommittableIncomingMessage].mapAsync(1)(m => run(m).recover(retry.handleFailures(m)))

  def run(m: CommittableIncomingMessage): Future[Outcome] = {
    def riskyOp(content: Int) = {
      val state = Random.nextInt(10)

      if (state > content)
        Future.failed(new IllegalStateException(s"$state is bigger than $content try later"))
      else Future.successful(state)
    }

    riskyOp(m.message.bytes.utf8String.toInt)
      .map(_ => SuccessfulMessage(m))
  }

  new RetryStream(
    settings = sourceSettings,
    queue = queue,
    bufferSize = 10).fromFlow(flow).runForeach(m => println(s"${System.currentTimeMillis()} -> $m"))


  println("ready")
}



