import java.io.{File, PrintWriter}
import java.util.Locale

import scala.collection.mutable
import scala.io.Source


class LanguageScorer private(frequencyMap: Map[String, Double], normFactor: Double, windowSize: Int) {

  def score(string: String): Double = {
    string
      .sliding(windowSize)
      .map(ngram => math.log(frequencyMap.getOrElse(ngram, 1.0 / normFactor)))
      .sum
  }

  def writeToFile(f: String): Unit = {
    val pw = new PrintWriter(f)

    pw.println("%.30f".format(normFactor))
    pw.println(windowSize)

    for ((string, frequency) <- frequencyMap) {
      pw.println(string + "," + "%.30f".format(frequency))
    }

    pw.close()
  }
}

object LanguageScorer {

  // TODO: naming of these methods??
  def readFromFile(file: File): LanguageScorer = {
    val lines = Source.fromFile(file).getLines().toList

    val normFactor = lines(0).toDouble
    val windowSize = lines(1).toInt

    val frequencies = for (line <- lines.drop(2)) yield {
      line.substring(0, windowSize) -> line.substring(windowSize + 1, line.length).toDouble
    }

    new LanguageScorer(frequencies.toMap, normFactor, windowSize)
  }

  def processFile(fileName: String, windowSize: Int): LanguageScorer = {
    val ngramMap: mutable.Map[String, Int] = mutable.Map()

    for {line <- Source.fromFile(fileName).getLines()
         ngram <- line.sliding(windowSize)} {
      ngramMap(ngram) = ngramMap.getOrElse(ngram, 0) + 1
    }

    // +1 for smoothing
    val normFactor: Double = ngramMap.foldLeft(0)(_ + _._2 + 1)
    val frequencyMap = ngramMap map { case (key, value) => (key, value / normFactor) }

    new LanguageScorer(frequencyMap.toMap, normFactor, windowSize)
  }

  def main(args: Array[String]) {

    val dir = "data/lang/"

    /*Locale.setDefault(new Locale("en", "US"));

    val eng = new NgramFrequencies()
    eng.process(dir + "1.en", 3)

    val ger = new NgramFrequencies()
    ger.process(dir + "1.de", 3)

    val hun = new NgramFrequencies()
    hun.process(dir + "1.hu", 3)

    val statGer = scala.io.Source.fromFile(dir + "top10000de.txt").getLines()
      .filter(_.length > 3).foldLeft((0.0, 0.0))((t, line) => (t._1 + 1, if (ger.score(line) > eng.score(line)) t._2
      + 1 else t._2))

    val statEng = scala.io.Source.fromFile(dir + "top10000en.txt").getLines()
      .filter(_.length > 3).foldLeft((0.0, 0.0))((t, line) => (t._1 + 1, if (ger.score(line) < eng.score(line)) t._2
      + 1 else t._2))

    println(statGer)
    println(statGer._2 / statGer._1)

    println(statEng)
    println(statEng._2 / statEng._1)

    eng.writeToFile(dir + "frequencies_en.dat")
    ger.writeToFile(dir + "frequencies_de.dat")

    val test = new NgramFrequencies
    test.readFromFile(dir + "frequencies_en.dat")

    val statTest = scala.io.Source.fromFile(dir + "top10000en.txt").getLines()
      .filter(_.length > 3).foldLeft((0.0, 0.0))((t, line) => (t._1 + 1, if (ger.score(line) < test.score(line)) t._2
       + 1 else t._2))

    println(test.frequencyMap.filter { case (key, value) => eng.frequencyMap.getOrElse(key, -1.0) != value})

    println(statTest)
    println(statTest._2 / statTest._1)*/

    val en = LanguageScorer.readFromFile(dir + "frequencies_en.dat")
    val de = LanguageScorer.readFromFile(dir + "frequencies_de.dat")

    val file = Source.fromFile(dir + "sentences.txt")

    file.getLines().foreach { line =>
      val scoreMap = Map("Eng: " -> en.score(line), "Ger: " -> de.score(line))
      println(scoreMap.maxBy(_._2)._1 + line)
    }

    file.close()
  }
}
