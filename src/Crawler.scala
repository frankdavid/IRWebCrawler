import java.net.{ConnectException, SocketTimeoutException}
import java.util.Locale

import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import scala.collection.JavaConversions._
import scala.collection.immutable.BitSet
import scala.collection.mutable


class Crawler(seedUrl: String) {

  val languageRecognizer = LanguageRecognizer.fromInputStreams(Seq(
    FileLoader.loadFileFromPathOrJar("data/frequencies_de.dat"),
    FileLoader.loadFileFromPathOrJar("data/frequencies_en.dat")
  ))

  val visitedUrls = new mutable.HashSet[String]()
  val enqueuedUrls = new mutable.HashSet[String]()
  val queue = new mutable.Queue[String]()
  val exceptionCount = new mutable.HashMap[String, Int]()
  val webPageHashes = new mutable.HashSet[String]()
  val simHashes = new mutable.HashSet[BitSet]()
  val md5Hasher = java.security.MessageDigest.getInstance("MD5")

  var exactDuplicates = 0
  var nearDuplicates = 0
  var englishPages = 0
  var englishPagesContainingStudent = 0

  /** Example: http://idvm-infk-hofmann03.inf.ethz.ch */
  val crawlDomain = {
    val regex = """^(.*:\/\/[^\/]+)\/.*""".r
    val regex(domain) = seedUrl
    domain
  }

  def normalizeAndFilterURLList(urlList: List[String]): Set[String] = {
    urlList
        .map(_.replaceAll("#.*", ""))
        .map(_.replaceAll("\\?.*", ""))
        .filter(url =>
          url.endsWith(".html") && url.startsWith(crawlDomain) && !url.matches(".*\\/login[a-f0-9]{4}\\.html"))
        .toSet
  }

  def getURLsFromDoc(doc: Document): Set[String] = {
    val links = doc.select("a[href]").map(x => x.attr("abs:href"))
    normalizeAndFilterURLList(links.toList)
  }

  def fetchDocumentFromURL(url: String): Document = {
    Jsoup.connect(url).timeout(5000).get()
  }

  def extractText(doc: Document): String = {
    if(doc.select("#contentMain, .textList").text.length > 10) {
      doc.select("#contentMain, .textList").text
    } else {
      doc.text
    }
  }

  def crawl(): CrawlResult = {
    queue.enqueue(seedUrl)
    enqueuedUrls += seedUrl
    val loggerThread = new LoggerThread()
    loggerThread.start()
    doCrawl()
    loggerThread.interrupt()
    CrawlResult(
      numDistinctUrls = visitedUrls.size,
      numExactDuplicates = exactDuplicates,
      numNearDuplicates = nearDuplicates,
      numEnglishPages = englishPages,
      studentFrequency = englishPagesContainingStudent)
  }

  private def processPage(url: String, doc: Document): Unit = {
    val text = Normalizer.normalize(extractText(doc))
    // we need an instance of Comparable for ConcurrentSkipList, hence convert to String first
    val isAlreadyPresent = !webPageHashes
        .add(new String(md5Hasher.digest(text.getBytes(doc.charset().displayName()))))
    if (isAlreadyPresent) {
      exactDuplicates += 1
    } else {
      val currentSimHash = SimHash128.getCodeOfDocument(text)
      if (simHashes.exists(existingSimHash => SimHash128.hammingDistance(existingSimHash, currentSimHash) <= 1)) {
        nearDuplicates += 1
      }
      simHashes.add(currentSimHash)
    }
    if (languageRecognizer.recognize(text) == Locale.ENGLISH) {
      englishPages += 1
      if (text.matches("(?i)(^|.*\\W)student(\\W.*|$)")) {
        // matches student, STudenT, does not match students etc.
        englishPagesContainingStudent += 1
      }
    }
  }

  private def doCrawl(): Unit = {
    while (queue.nonEmpty) {
      val url = queue.dequeue()
      try {
        val doc = fetchDocumentFromURL(url)
        visitedUrls += url
        processPage(url, doc)
        for (url <- getURLsFromDoc(doc).diff(enqueuedUrls)) {
          queue.enqueue(url)
          enqueuedUrls += url
        }
      }
      catch {
        case e: org.jsoup.HttpStatusException =>
          val message = "status" + e.getStatusCode
          exceptionCount(message) = exceptionCount.getOrElse(message, 0) + 1
        case _: ConnectException =>
          queue.add(url)
        case _: SocketTimeoutException =>
          queue.add(url)
        case e: Throwable =>
          val message = e.getClass.getName + " " + e.getMessage
          exceptionCount(message) = exceptionCount.getOrElse(message, 0) + 1
      }
    }
  }

  class LoggerThread extends Thread {

    override def run(): Unit = {
      try {
        while (!isInterrupted) {
          Thread.sleep(1500)
          val remaining = queue.size + 1 // + 1 for in progress
          println(s"Crawled: ${visitedUrls.size}, remaining: $remaining")
        }
      } catch {
        case _: InterruptedException => // NOOP
      }
    }
  }
}

object Crawler {

  def main(args: Array[String]) {
    val seed = args.headOption.getOrElse("http://idvm-infk-hofmann03.inf.ethz.ch/eth/www.ethz.ch/en.html")
    val result = new Crawler(seed).crawl()

    println(s"Distinct urls: ${result.numDistinctUrls}")
    println(s"Exact duplicates: ${result.numExactDuplicates}")
    println(s"Near duplicates: ${result.numNearDuplicates}")
    println(s"English pages: ${result.numEnglishPages}")
    println(s"Student frequency: ${result.studentFrequency}")
  }
}

