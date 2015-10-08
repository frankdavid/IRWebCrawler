import java.io.File
import java.net.{ConnectException, SocketTimeoutException}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListSet, LinkedBlockingQueue}
import java.util.{Collections, Locale}

import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import scala.collection.JavaConversions._
import scala.collection.immutable.BitSet


class Crawler(seedUrl: String) {

  val languageRecognizer = LanguageRecognizer.fromFiles(Seq(
    new File("data/lang/frequencies_de.dat"),
    new File("data/lang/frequencies_en.dat")
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
        .filter(url => url.endsWith(".html") && url.startsWith(crawlDomain))
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
      simHashes.add(currentSimHash)  // TODO: should we add the current even if we found a near duplicate?
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
    threads.foreach(_.join())
    println(exceptionCount)
    CrawlResult(
      numDistinctUrls = visitedUrls.size,
      numExactDuplicates = exactDuplicates.get,
      numNearDuplicates = nearDuplicates.get,
      numEnglishPages = englishPages.get,
      studentFrequency = englishPagesContainingStudent.get)
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
            //println(id, visitedUrls.size, queue.size, url)

            visitedUrls += url
            processPage(url, doc)
            if (visitedUrls.size % 100 == 0) {
              println(visitedUrls.size, queue.size)
            }
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
              e.printStackTrace()
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

//    var a = fetchDocumentFromURL("http://idvm-infk-hofmann03.inf.ethz.ch/eth/www.ethz.ch/en/studies/non-degree-courses/exchange-and-visiting-studies/programmes.html")
//    println(extractText(a).length, extractText(a))

    val seed = args.headOption.getOrElse("http://idvm-infk-hofmann03.inf.ethz.ch/eth/www.ethz.ch/en.html")
    val result = new Crawler(seed).crawl()

    println(s"Distinct urls: ${result.numDistinctUrls}")
    println(s"Exact duplicates: ${result.numExactDuplicates}")
    println(s"Near duplicates: ${result.numNearDuplicates}")
    println(s"English pages: ${result.numEnglishPages}")
    println(s"Student frequency: ${result.studentFrequency}")

//    var ls = linkContent.keys.toList zip linkContent.values.map(x => SimHash128.getCodeOfDocument(x, 5))
//    println("Starting comparison")
//    for (d1 <- ls; d2 <- ls) {
//      if (d1._1 != d2._1 && SimHash128.compareCodes(d1._2, d2._2) > 120) {
//        println(SimHash128.compareCodes(d1._2, d2._2), " ", d1._1, " ", d2._1)
//      }
//    }

    //    val simhash = SimHash
    //
    //    val scores = linkContent.mapValues(x => simhash.getCodeOfDocument(x, 5))
    //    println("Starting Score Calc")
    //    var rank = for (d1 <- scores.values.toList.zipWithIndex; d2 <- scores.values.toList.zipWithIndex) yield (simhash.compareCodes(d1._1, d2._1), d1._2, d2._2)
    //    println("Finished Score Calc")
    //    println(rank.filter{case(a, d1, d2) => d1!=d2}.toList.sorted.reverse.take(5))
  }
}

