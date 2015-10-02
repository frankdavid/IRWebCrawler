import java.io.{FileOutputStream, OutputStreamWriter, BufferedWriter, PrintWriter}
import java.util.Locale

import scala.collection.mutable.Map

/**
 * Created by machine on 13.09.2015.
 */

object LanguageRecognizer {

  class NgramFrequencies() {

    var frequencyMap:Map[String,Double] = Map()
    var normFactor:Double = 0.0
    var win:Int = 3

    def process(file: String, windowSize: Int): Unit = {

      win = windowSize

      val ngramMap:Map[String,Int] = Map()

      for{line <- scala.io.Source.fromFile(file, "UTF-8").getLines(); ngram <- line.sliding(win)}
        ngramMap.update(ngram, ngramMap.getOrElse(ngram, 0) + 1)

      //+1 for smoothing
      normFactor = ngramMap.foldLeft(0)(_ + _._2 + 1)

      frequencyMap = ngramMap map {case (key, value) => (key, value / normFactor)}
    }

    def score(s: String) =
      s.sliding(win).foldLeft(0.0)((sum, ngram) => sum + Math.log(frequencyMap.getOrElse(ngram, 1.0 / normFactor)))

    def writeToFile(f: String) = {

      val pw = new PrintWriter(new OutputStreamWriter(
        new FileOutputStream(f), "UTF-8"));

      pw.println("%.30f".format(normFactor))
      pw.println(win)
      frequencyMap.foreach(p => pw.println(p._1 + "," + "%.30f".format(p._2)))

      pw.close()
    }

    def readFromFile(f: String) = {

      val lines = scala.io.Source.fromFile(f, "UTF-8").getLines().toList

      normFactor = lines(0).toDouble
      win = lines(1).toInt

      lines.drop(2).foreach(line =>
        frequencyMap.put(line.substring(0, win), line.substring(win + 1, line.length).toDouble))
    }
  }

  def main (args: Array[String]) {

    val dir = "data/lang/"

    Locale.setDefault(new Locale("en", "US"));

    val eng = new NgramFrequencies()
    eng.process(dir + "1.en", 3)

    val ger = new NgramFrequencies()
    ger.process(dir + "1.de", 3)

    val hun = new NgramFrequencies()
    hun.process(dir + "1.hu", 3)

    val statGer = scala.io.Source.fromFile(dir + "top10000de.txt").getLines()
      .filter(_.length > 3).foldLeft((0.0, 0.0))((t, line) => (t._1 + 1, if (ger.score(line) > eng.score(line)) t._2 + 1 else t._2))

    val statEng = scala.io.Source.fromFile(dir + "top10000en.txt").getLines()
      .filter(_.length > 3).foldLeft((0.0, 0.0))((t, line) => (t._1 + 1, if (ger.score(line) < eng.score(line)) t._2 + 1 else t._2))

    println(statGer)
    println(statGer._2 / statGer._1)

    println(statEng)
    println(statEng._2 / statEng._1)

    eng.writeToFile(dir + "frequencies_en.dat")
    ger.writeToFile(dir + "frequencies_de.dat")

    val test = new NgramFrequencies
    test.readFromFile(dir + "frequencies_en.dat")

    val statTest = scala.io.Source.fromFile(dir + "top10000en.txt").getLines()
      .filter(_.length > 3).foldLeft((0.0, 0.0))((t, line) => (t._1 + 1, if (ger.score(line) < test.score(line)) t._2 + 1 else t._2))

    println(test.frequencyMap.filter { case (key, value) => eng.frequencyMap.getOrElse(key, -1.0) != value})

    println(statTest)
    println(statTest._2 / statTest._1)

    val en = new NgramFrequencies
    val de = new NgramFrequencies

    en.readFromFile(dir + "frequencies_en.dat")
    de.readFromFile(dir + "frequencies_de.dat")

    scala.io.Source.fromFile(dir + "sentences.txt").getLines().foreach(line => {

      val scoreMap = Map("Eng: " -> en.score(line), "Ger: " -> de.score(line))

      println(scoreMap.maxBy(_._2)._1 + line)
    })
  }
}