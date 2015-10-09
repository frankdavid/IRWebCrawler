import java.io.{InputStream, File, PrintWriter}
import java.util.Locale

import scala.collection.mutable
import scala.io.Source


class LanguageScorer private
    (val language: Locale, private val frequencyMap: Map[String, Double], normFactor: Double, windowSize: Int) {

  def score(string: String): Double = {
    string
      .sliding(windowSize)
      .map(ngram => math.log(frequencyMap.getOrElse(ngram, 1.0 / normFactor)))
      .sum
  }

  def writeToFile(file: File): Unit = {
    val pw = new PrintWriter(file)

    pw.println(language.toLanguageTag)
    pw.println("%.30f".format(normFactor))
    pw.println(windowSize)

    for ((string, frequency) <- frequencyMap) {
      pw.println(string + "," + "%.30f".format(frequency))
    }

    pw.close()
  }
}

object LanguageScorer {

  def deserialize(inputStream: InputStream): LanguageScorer = {
    val lines = Source.fromInputStream(inputStream, "UTF-8").getLines().toList

    val language = Locale.forLanguageTag(lines(0))
    val normFactor = lines(1).toDouble
    val windowSize = lines(2).toInt

    val frequencies = for (line <- lines.drop(3)) yield {
      line.substring(0, windowSize) -> line.substring(windowSize + 1, line.length).toDouble
    }

    inputStream.close()
    new LanguageScorer(language, frequencies.toMap, normFactor, windowSize)
  }

  def trainFromText(language: Locale, input: InputStream, windowSize: Int): LanguageScorer = {
    val ngramMap: mutable.Map[String, Int] = mutable.Map()

    for {line <- Source.fromInputStream(input, "UTF-8").getLines()
         ngram <- line.sliding(windowSize)} {
      ngramMap(ngram) = ngramMap.getOrElse(ngram, 0) + 1
    }

    // +1 for smoothing
    val normFactor: Double = ngramMap.foldLeft(0)(_ + _._2 + 1)
    val frequencyMap = ngramMap map { case (key, value) => (key, value / normFactor) }

    new LanguageScorer(language, frequencyMap.toMap, normFactor, windowSize)
  }
}
