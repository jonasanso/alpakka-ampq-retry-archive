package retry

import akka.stream.Materializer
import akka.stream.alpakka.amqp.scaladsl._
import akka.stream.alpakka.amqp.{AmqpSinkSettings, Declaration, IncomingMessage, NamedQueueSourceSettings, OutgoingMessage, QueueDeclaration, Seq}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.{Done, NotUsed}
import com.rabbitmq.client.AMQP.BasicProperties

import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

sealed trait Outcome {
  def message: CommittableIncomingMessage
}

sealed trait ErrorOutcome extends Outcome {
  def outgoing: OutgoingMessage
}

case class SuccessfulMessage(message: CommittableIncomingMessage) extends Outcome

case class RetryMessage(message: CommittableIncomingMessage, outgoing: OutgoingMessage) extends ErrorOutcome

case class ArchiveMessage(message: CommittableIncomingMessage, outgoing: OutgoingMessage) extends ErrorOutcome


class Retry(delays: FiniteDuration*) extends IncomingMessageExtensions {
  def handleFailures(m: CommittableIncomingMessage): PartialFunction[Throwable, Outcome] = {
    case NonFatal(_) =>
      retryOrArchive(m)
  }

  private def retryOrArchive(m: CommittableIncomingMessage): ErrorOutcome = {
    val retryCount = m.message.retryCount
    if (retryCount < delays.length) {
      val expiration = delays(retryCount)
      RetryMessage(m, m.message.nextRetry(expiration))
    } else {
      ArchiveMessage(m, m.message.archive)
    }
  }

}

trait IncomingMessageExtensions {
  val RETRY_COUNT_HEADER = "x-retry-count"

  import scala.collection.JavaConverters._

  implicit class ExtendIncomingMessage(m: IncomingMessage) {
    // properties.getHeaders returns an Collections.unmodifiableMap that we cannot change unless we copy it
    private val headers = Option(m.properties.getHeaders)
      .map(_.asScala.toMap)
      .getOrElse(immutable.Map[String, AnyRef]())

    private def outgoing(props: BasicProperties) =
      OutgoingMessage(
        bytes = m.bytes,
        immediate = false,
        mandatory = false,
        props = Some(props))

    val retryCount: Int = {
      (for {
        retryCountUntyped <- Try(headers.get(RETRY_COUNT_HEADER)).toOption.flatten
        retryCount <- Try(retryCountUntyped.asInstanceOf[Integer].toInt).toOption
      } yield retryCount).getOrElse(0)
    }

    def nextRetry(expiration: FiniteDuration): OutgoingMessage = {
      val newProps = m.properties.builder()
        .headers((headers + (RETRY_COUNT_HEADER -> new Integer(retryCount + 1))).asJava)
        .expiration(expiration.toMillis.toString)
        .build()

      outgoing(newProps)
    }

    def archive: OutgoingMessage = outgoing(m.properties)
  }

}

class RetryStream(settings: NamedQueueSourceSettings,
                  queue: RetryQueue,
                  bufferSize: Int)(implicit mat: Materializer, ec: ExecutionContext) {

  def fromFlow(flow: Flow[CommittableIncomingMessage, Outcome, _]): Source[Outcome, NotUsed] =
    source.via(flow).mapAsync(1)(ack)

  private val source: Source[CommittableIncomingMessage, NotUsed] =
    AmqpSource.committableSource(settings, bufferSize)

  private val retrySink: Sink[OutgoingMessage, Future[Done]] = AmqpSink(
    AmqpSinkSettings(settings.connectionSettings).withRoutingKey(queue.retryQueueName)
  )

  private val archiveSink: Sink[OutgoingMessage, Future[Done]] = AmqpSink(
    AmqpSinkSettings(settings.connectionSettings).withRoutingKey(queue.archiveQueueName)
  )

  private def ack(m: Outcome): Future[Outcome] = {
    val sendOutgoingIfNeeded = m match {
      case RetryMessage(_, o) => send(o, retrySink)
      case ArchiveMessage(_, o) => send(o, archiveSink)
      case _ => Future.successful(Done)
    }

    for {
      _ <- sendOutgoingIfNeeded
      _ <- m.message.ack()
    } yield m
  }

  private def send(o: OutgoingMessage, sink: Sink[OutgoingMessage, Future[Done]]): Future[Done] = {
    Source.single(o).runWith(sink)
  }
}

case class RetryQueue(name: String, durable: Boolean = true) {
  val retryQueueName = s"$name.retry"
  val archiveQueueName = s"$name.archived"

  private val main = QueueDeclaration(name, durable = durable)

  private val retry = QueueDeclaration(retryQueueName, durable = durable, arguments = Map(
    "x-dead-letter-exchange" -> "", // This is a special exchange where the routing key is the queue where our message should end after it will expire (the message TTL)
    "x-dead-letter-routing-key" -> name
  ))
  private val archive = QueueDeclaration(archiveQueueName, durable = durable, arguments = Map(
    "x-expires" -> new java.lang.Long(2592000000L),
    "x-max-length" -> new java.lang.Long(1000000L)))

  val declarations: Seq[Declaration] =
    List(main, retry, archive)

}
