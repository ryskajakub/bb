package controllers

import play.api._
import play.api.mvc._
import play.api.libs.json._

import akka.stream.scaladsl.Source

import javax.inject._
import java.util.UUID
import java.util.concurrent.ForkJoinPool
import java.text.SimpleDateFormat
import java.util.Date

import scala.concurrent.{Future, Promise, ExecutionContext}

import scala.collection.JavaConverters
import scala.collection.mutable.Buffer

import com.amazonaws.handlers.AsyncHandler

import com.amazonaws.regions.Regions

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.AWSStaticCredentialsProvider

import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.AmazonSQSAsyncClientBuilder

import com.amazonaws.services.sqs.model.SendMessageBatchRequest
import com.amazonaws.services.sqs.model.SendMessageBatchResult
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry
import com.amazonaws.services.sqs.model.ReceiveMessageResult
import com.amazonaws.services.sqs.model._


/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class HomeController @Inject()(cc: ControllerComponents) extends AbstractController(cc) {

  val awsEc = ExecutionContext.fromExecutor(
    new ForkJoinPool())

  lazy val (sqs, queueUrl) = {
    val queueName = "test"
    val awsSecret = sys.env("SECRET")
    val awsKey = sys.env("KEY")
    val credentialsProvider = new AWSStaticCredentialsProvider(
      new BasicAWSCredentials (
        awsKey ,
        awsSecret ))
    val queue = AmazonSQSAsyncClientBuilder
      .standard().withRegion(Regions.US_EAST_1)
        .withCredentials(credentialsProvider).build()
    val url = try { 
      queue.getQueueUrl(
        new GetQueueUrlRequest(queueName)).getQueueUrl
    } catch {
      case t: QueueDoesNotExistException =>
        queue.createQueue(new CreateQueueRequest(queueName)).getQueueUrl
    }
    (queue, url)
  }

  def index() = Action {
    Ok(views.html.index())
  }

  val messageDateFormat = 
    new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")

  def mkMessage () = {
    val uuid = UUID.randomUUID().toString()
    val date = messageDateFormat.format(new Date)
    val messageBody = JsObject (
      Map (
        ("uuid", JsString(uuid)) ,
        ("createdAt", JsString(date))
        )
      )
    new SendMessageBatchRequestEntry(
      uuid, 
      Json.stringify(messageBody));
  }

  val writeBatchSize = 10   
  val writeBatchesCount = 1000 

  def pushMessages() = Action.async {

    val allRequests = (0 until writeBatchesCount).map(_ => {
      val p = Promise[Unit]()
      val entries = (0 until writeBatchSize).map(_ => mkMessage)
      val request = new SendMessageBatchRequest().withQueueUrl(queueUrl).withEntries(JavaConverters.asJavaCollection(entries))
      val handler = new AsyncHandler[SendMessageBatchRequest,
          SendMessageBatchResult] {
        def onError(e: Exception) = p.failure(e)
        def onSuccess(req: SendMessageBatchRequest, 
            res: SendMessageBatchResult) = 
          p.success(())
      }
      sqs.sendMessageBatchAsync(request, handler)
      p.future
    })

    implicit val c = awsEc
    Future.sequence(allRequests).map(_ => Ok("ok"))

  }

  def mkHandler[Req <: com.amazonaws.AmazonWebServiceRequest,Res](): 
      (Future[Res], AsyncHandler[Req, Res]) = {
    val p = Promise[Res]()
    val handler = 
      new AsyncHandler[Req, Res] {
        def onError(e: Exception) = p.failure(e)
        def onSuccess(req: Req, res: Res) {
          p.success(res)
        }
      }
    (p.future, handler)
  }

  def status() = Action.async {
    val messageType = "ApproximateNumberOfMessages"
    val messageTypes = JavaConverters.bufferAsJavaList(
      Buffer(messageType))
    val (f, handler) = mkHandler[GetQueueAttributesRequest, 
      GetQueueAttributesResult]()
    sqs.getQueueAttributesAsync(queueUrl, messageTypes, handler)
    f.map{x => 
      (JavaConverters.mapAsScalaMap
          (x.getAttributes).get(messageType)) match {
        case Some(y) => Ok(y)
        case None => InternalServerError
      }
    }(awsEc)
  }

  val runs = 20
  val readBatchSize = 5000

  def readMessages() = Action {

    def readMessage(countdown: Int)
        (messages: Seq[Message]): 
        Option[(Int,String)] = {
      countdown match {
        case 0 => None
        case _ => {
          val messagesLength = messages.length
          val beginning = (countdown, messagesLength) match {
            case (`runs`, _) => "["
            case (_, 0) => ""
            case (1, _) => ","
            case _ => ","
          }
          val end = (countdown, messagesLength) match {
            case (_, 0) => "]"
            case (1, _) => "]"
            case _ => ""
          }
          val asJson = messages.map(_.getBody()).mkString(",")
          val newState = if (messagesLength == 0) { -1 } 
              else { countdown - 1 }  
          Some(newState, beginning + asJson + end)
        }
      }
    }
    
    def receiveMessage(countdown: Int): 
        Future[Option[(Int, String)]] = {
      val request = new ReceiveMessageRequest(queueUrl)
        .withMaxNumberOfMessages(10)
      val sentRequests: Seq[Future[ReceiveMessageResult]] = ((0 until readBatchSize).map { _ =>
        val (f, handler) = 
          mkHandler[ReceiveMessageRequest, ReceiveMessageResult] ()
        sqs.receiveMessageAsync(request, handler)
        f
      })
      implicit val c = awsEc
      val batchedSentRequests:Future[Seq[Message]] = 
        Future.sequence(sentRequests).map(_.flatMap(x =>
          JavaConverters.asScalaBuffer(x.getMessages())))
      batchedSentRequests.map(readMessage(countdown))(awsEc)
    }

    val source = Source.unfoldAsync(runs)(x => receiveMessage(x))
    Ok.chunked(source).as("application/json")

  }

}
