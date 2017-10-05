package controllers

import play.api._
import play.api.mvc._
import play.api.libs.json._

import javax.inject._
import java.util.UUID
import java.util.concurrent.ForkJoinPool

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

  def mkMessage () = {
    val uuid = UUID.randomUUID().toString()
    val messageBody = JsObject (
      Map (
        ("uuid", JsString(uuid)),
        ("createdAt", JsString (""))
        )
      )
    new SendMessageBatchRequestEntry(
      uuid, 
      Json.stringify(messageBody));
  }

  val batchSize = 2
  val batchesCount = 2

  def pushMessages() = Action.async {

    val allRequests = (0 until batchesCount).map(_ => {
      val p = Promise[Unit]()
      val entries = (0 until batchSize).map(_ => mkMessage)
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
    f.map{x => System.out.println(x.getAttributes); Ok("ok")}(awsEc)
  }

  def readMessages() = Action.async {

    def readMessage(result: ReceiveMessageResult): Unit = {
      JavaConverters.asScalaBuffer(result.getMessages()).foreach{m =>
        System.out.println(m.getBody())
      }
    }
    
    def receiveMessage: Future[ReceiveMessageResult] = {
      val p = Promise[ReceiveMessageResult]()
      val request = new ReceiveMessageRequest(queueUrl)
      val handler = new AsyncHandler[ReceiveMessageRequest,
          ReceiveMessageResult] {
        def onError(e: Exception) = p.failure(e)
        def onSuccess(req: ReceiveMessageRequest, 
            res: ReceiveMessageResult) = p.success(res)
      }
      sqs.receiveMessageAsync(request, handler)
      p.future
    }

    receiveMessage.map{m => readMessage(m); Ok("ok")}(awsEc)

    /*
    def repeatTillEnd () = {
      receiveMessage.flatMap { rmr =>
        
      }
      repeatTillEnd
    }
    */

  }

}
