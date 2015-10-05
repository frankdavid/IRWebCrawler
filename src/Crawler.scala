import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListSet, LinkedBlockingQueue}

import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import scala.collection.JavaConversions._
import scala.collection.parallel.mutable.ParHashMap


object Crawler {

  var seedUrl: String = "http://idvm-infk-hofmann03.inf.ethz.ch/eth/www.ethz.ch/"
  var visitedUrls = new ConcurrentSkipListSet[String]()
  var enquedUrls = new ConcurrentSkipListSet[String]()
  var queue = new LinkedBlockingQueue[String]()
  var linkContent = new ConcurrentHashMap[String, String]()

  val exceptionCount = new ParHashMap[String, Int]()

  def filterURLList(url_list: List[String]): Set[String] = {

    var url_list_normalized = url_list.map(x => x.replaceAll("#.*", ""))

    //TODO investigate this lines influence on neighbbours
    url_list_normalized = url_list_normalized.map(x => x.replaceAll("\\?.*", ""))

    //sort unsupported ends
    url_list_normalized = url_list_normalized.filter(x => x.matches(".*(\\.html?|\\/[^\\/\\.]*)$"))
//    url_list_normalized = url_list_normalized.filter(x => (!x.endsWith(".png") &&
//      !x.endsWith(".jpg") && !x.endsWith(".pdf") && !x.endsWith("doc") && !x.endsWith(".docx")))


    //filter out url pointing to outside
    return url_list_normalized.filter(x => x.startsWith(seedUrl)).toSet
  }

  def getURLsFromDoc(doc: Document): Set[String] = {
    var links = doc.select("a[href]").map(x => x.attr("abs:href"))
    return filterURLList(links.toList)
  }

  def fetchDocumentFromURL(url: String): Document = {
    return Jsoup.connect(url).timeout(5000).get()
    //    return Jsoup.parse(Source.fromURL(url).mkString)
  }

  def extractText(doc: Document): String = {
    if(doc.select("#contentMain").text.length > 100) {
      return doc.select("#contentMain").text
    }
    return doc.text
  }

  class CrawlerThread(id: Int) extends Runnable {

    def run {
      while (queue.nonEmpty) {
        val url = queue.poll()
        try {
          var doc = fetchDocumentFromURL(url)
          //println(id, visitedUrls.size, queue.size, url)

          linkContent(url) = Normalizer.normalize(extractText(doc))

            visitedUrls += url
            if (visitedUrls.size % 100 == 0) {
              println(visitedUrls.size, queue.size)
            }
          for (url <- getURLsFromDoc(doc).filter(x => !enquedUrls.contains(x))) {
              queue.add(url)
              enquedUrls += url
          }
        }
        catch {
          case e: org.jsoup.HttpStatusException =>
            val message = "status" + e.getStatusCode
            exceptionCount(message) = exceptionCount.getOrElse(message, 0) + 1
          case e: Throwable =>
            e.printStackTrace()
            val message = e.getClass.getName + " " + e.getMessage
            exceptionCount(message) = exceptionCount.getOrElse(message, 0) + 1
        }

      }
    }

  }


  def bfsFromSeed(seed: String): Unit = {

    queue.add(seed)
    enquedUrls += seed
    val threads = for(i <- 1 to 10) yield {
      val t = new Thread(new CrawlerThread(i))
      t.start()
      Thread.sleep(1000)
      t
    }
    threads.foreach(_.join())

    println(exceptionCount)
  }


  def main(args: Array[String]) {

//    var a = fetchDocumentFromURL("http://idvm-infk-hofmann03.inf.ethz.ch/eth/www.ethz.ch/en/studies/non-degree-courses/exchange-and-visiting-studies/programmes.html")
//    println(extractText(a).length, extractText(a))

    var seed = "http://idvm-infk-hofmann03.inf.ethz.ch/eth/www.ethz.ch/en.html"
    bfsFromSeed(seed)
    println(linkContent.size)

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

