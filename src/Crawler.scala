import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import scala.collection.JavaConversions._
import scala.collection.mutable


object Crawler {

  var seedUrl: String = "http://idvm-infk-hofmann03.inf.ethz.ch/eth/www.ethz.ch/"
  var visitedUrls: mutable.Set[String] = mutable.Set()
  var enquedUrls: mutable.Set[String] = mutable.Set[String]()
  var queue: mutable.Queue[String] = mutable.Queue[String]()
  var linkContent: mutable.Map[String, String] = mutable.Map[String, String]()

  def filterURLList(url_list: List[String]): Set[String] = {

    var url_list_normalized = url_list.map(x => x.replaceAll("#.*", ""))

    //TODO investigate this lines influence on neighbbours
    url_list_normalized = url_list_normalized.map(x => x.replaceAll("\\?.*", ""))

    //sort unsupported ends
    url_list_normalized = url_list_normalized.filter(x => x.endsWith(".html") || (!x.endsWith(".png") &&
      !x.endsWith(".jpg") && !x.endsWith(".pdf") && !x.endsWith("doc") && !x.endsWith(".docx")))


    //filter out url pointing to outside
    return url_list_normalized.filter(x => x.startsWith(seedUrl)).toSet
  }

  def getURLsFromDoc(doc: Document): Set[String] = {
    var links = doc.select("a[href]").map(x => x.attr("abs:href"))
    return filterURLList(links.toList)
  }

  def fetchDocumentFromURL(url: String): Document = {
    return Jsoup.connect(url).timeout(60 * 1000).get()
    //    return Jsoup.parse(Source.fromURL(url).mkString)
  }

  def extractText(doc: Document): String = {
    doc.text()
  }

  class CrawlerThread(id: Int) extends Runnable {

    def run {
      while (queue.nonEmpty && linkContent.size < 1000) {
        var url = ""
        synchronized {
          url = queue.dequeue()
        }
        try {
          var doc = fetchDocumentFromURL(url)
          //println(id, visitedUrls.size, queue.size, url)

          linkContent(url) = Normalizer.normalize(extractText(doc))

          synchronized {
            visitedUrls.add(url)
            if (visitedUrls.size % 100 == 0) {
              println(visitedUrls.size, queue.size)
            }
          }
          for (url <- getURLsFromDoc(doc).filter(x => !enquedUrls.contains(x))) {
            synchronized {
              queue.enqueue(url)
              enquedUrls.add(url)
            }
          }
        }
        catch {
          case e1: org.jsoup.UnsupportedMimeTypeException => if (!url.endsWith(".png") && !url.endsWith(".pdf")) println("Could not read url " + url)
          case e2: org.jsoup.HttpStatusException => println("Status exception " + url)
          case e: Exception => println("BAAD", e.getMessage, url)
        }

      }
    }

  }


  def bfsFromSeed(seed: String): Unit = {

    queue.enqueue(seed)
    enquedUrls.add(seed)
    var threads = mutable.MutableList[Thread]()
    for (i <- 1 to 1) {
      var t: Thread = new Thread(new CrawlerThread(i))
      threads += t
      t.start
      Thread.sleep(1000)
    }
    for (t <- threads) {
      t.join()
    }


  }


  def main(args: Array[String]) {

    var seed = "http://idvm-infk-hofmann03.inf.ethz.ch/eth/www.ethz.ch/en.html"
    bfsFromSeed(seed)
    println(linkContent.size)

    var ls = linkContent.keys.toList zip linkContent.values.map(x => SimHash128.getCodeOfDocument(x, 5))
    println("Starting comparison")
    for (d1 <- ls; d2 <- ls) {
      if (d1._1 != d2._1 && SimHash128.compareCodes(d1._2, d2._2) > 124) {
        println(SimHash128.compareCodes(d1._2, d2._2), " ", d1._1, " ", d2._1)
      }
    }

    //    val simhash = SimHash
    //
    //    val scores = linkContent.mapValues(x => simhash.getCodeOfDocument(x, 5))
    //    println("Starting Score Calc")
    //    var rank = for (d1 <- scores.values.toList.zipWithIndex; d2 <- scores.values.toList.zipWithIndex) yield (simhash.compareCodes(d1._1, d2._1), d1._2, d2._2)
    //    println("Finished Score Calc")
    //    println(rank.filter{case(a, d1, d2) => d1!=d2}.toList.sorted.reverse.take(5))
  }
}

