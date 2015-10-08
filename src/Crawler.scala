import java.net.SocketTimeoutException
import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListSet, LinkedBlockingQueue}

import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import scala.collection.JavaConversions._
import scala.collection.parallel.mutable.ParHashMap


class Crawler(seedUrl: String) {

  val visitedUrls = new ConcurrentSkipListSet[String]()
  val enqueuedUrls = new ConcurrentSkipListSet[String]()
  val inProgress = new ConcurrentSkipListSet[String]()
  val queue = new LinkedBlockingQueue[String]()
  val linkContent = new ConcurrentHashMap[String, String]()
  val exceptionCount = new ParHashMap[String, Int]()

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
        .filter(_.matches(".*(\\.html?|\\/[^\\/\\.]*)$")) //sort unsupported ends
        .filter(_.startsWith(crawlDomain)) //filter out url pointing to outside
        .filter(!_.contains("/login"))
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

  def processPage(url: String, doc: Document) = {
    linkContent(url) = Normalizer.normalize(extractText(doc))
  }

  def crawl(): Unit = {
    queue.add(seedUrl)
    enqueuedUrls += seedUrl
    val threads = for (i <- 1 to 1) yield {
      val t = new CrawlerThread(i)
      t.start()
      t
    }
    threads.foreach(_.join())
    println(exceptionCount)
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
    val crawler = new Crawler(seed)
    crawler.crawl()
    println(crawler.linkContent.size)

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

