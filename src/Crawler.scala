import java.net.{ConnectException, SocketTimeoutException}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListSet, LinkedBlockingQueue}
import java.util.{Collections, Locale}

import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import scala.collection.JavaConversions._
import scala.collection.immutable.BitSet


class Crawler(seedUrl: String) {

  val languageRecognizer = LanguageRecognizer.fromInputStreams(Seq(
    FileLoader.loadFileFromPathOrJar("data/frequencies_de.dat"),
    FileLoader.loadFileFromPathOrJar("data/frequencies_en.dat")
  ))

  val visitedUrls = new ConcurrentSkipListSet[String]()
  val enqueuedUrls = new ConcurrentSkipListSet[String]()
  val inProgress = new ConcurrentSkipListSet[String]()
  val queue = new LinkedBlockingQueue[String]()
  val linkContent = new ConcurrentHashMap[String, String]()
  val exceptionCount = new ConcurrentHashMap[String, Int]()
  val md5Hasher = java.security.MessageDigest.getInstance("MD5")
  val webPageHashes = new ConcurrentSkipListSet[String]()
  // skip list only accepts Comparable instances, we cannot use that here
  val simHashes = Collections.newSetFromMap(new ConcurrentHashMap[BitSet, java.lang.Boolean]())

  val exactDuplicates = new AtomicInteger()
  val nearDuplicates = new AtomicInteger()
  val englishPages = new AtomicInteger()
  val englishPagesContainingStudent = new AtomicInteger()

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

  def processPage(url: String, doc: Document): Unit = {
    val text = Normalizer.normalize(extractText(doc))
    linkContent(url) = text
    // we need an instance of Comparable for ConcurrentSkipList, hence convert to String first
    val isAlreadyPresent = !webPageHashes.add(new String(md5Hasher.digest(text.getBytes(doc.charset().displayName()))))
    if (isAlreadyPresent) {
      exactDuplicates.incrementAndGet()
    } else {
      val currentSimHash = SimHash128.getCodeOfDocument(text)
      if (simHashes.exists(existingSimHash => SimHash128.hammingDistance(existingSimHash, currentSimHash) <= 1)) {
        nearDuplicates.incrementAndGet()
      }
      simHashes.add(currentSimHash)
    }
    if (languageRecognizer.recognize(text) == Locale.ENGLISH) {
      englishPages.incrementAndGet()
      if (text.matches("(?i)(^|.*\\W)student(\\W.*|$)")) { // matches student, STudenT, does not match students etc.
        englishPagesContainingStudent.incrementAndGet()
      }
    }
  }

  def crawl(): CrawlResult = {
    queue.add(seedUrl)
    enqueuedUrls += seedUrl
    val threads = for (i <- 1 to 50) yield {
      val t = new CrawlerThread(i)
      t.start()
      t
    }
    val loggerThread = new LoggerThread()
    loggerThread.start()
    threads.foreach(_.join())
    loggerThread.interrupt()
    CrawlResult(
      numDistinctUrls = visitedUrls.size,
      numExactDuplicates = exactDuplicates.get,
      numNearDuplicates = nearDuplicates.get,
      numEnglishPages = englishPages.get,
      studentFrequency = englishPagesContainingStudent.get)
  }

  class LoggerThread extends Thread {

    override def run(): Unit = {
      try {
        while (!isInterrupted) {
          Thread.sleep(1500)
          println(s"Crawled: ${visitedUrls.size}, remaining: ${queue.size}")
        }
      } catch {
        case _: InterruptedException => // NOOP
      }
    }
  }

  class CrawlerThread(id: Int) extends Thread {

    override def run(): Unit = {
      while (true) {
        val url = inProgress.synchronized {
          val url = queue.poll()
          if (url != null) {
            inProgress.add(url)
          }
          url
        }
        if (url == null) {
          if (inProgress.isEmpty) {
            return
          } else {
            Thread.sleep(200)
          }
        } else {
          try {
            val doc = fetchDocumentFromURL(url)

            visitedUrls += url
            processPage(url, doc)
            for (url <- getURLsFromDoc(doc).filter(x => !enqueuedUrls.contains(x))) {
              queue.add(url)
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
          inProgress -= url
        }
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

