import java.io.File
import java.util.Locale

import scala.io.Source

object LanguageScorerTest {

  val dir = "data/lang/"

  def main(args: Array[String]) {
    val english = LanguageScorer.trainFromText(Locale.ENGLISH, new File(dir + "1.en"), 3)

    val german = LanguageScorer.trainFromText(Locale.GERMAN, new File(dir + "1.de"), 3)

    val statGer = Source.fromFile(dir + "top10000de.txt").getLines()
        .filter(_.length > 3).foldLeft((0.0, 0.0))((t, line) =>
      (t._1 + 1, if (german.score(line) > english.score(line)) t._2 + 1 else t._2))

    val statEng = Source.fromFile(dir + "top10000en.txt").getLines()
        .filter(_.length > 3).foldLeft((0.0, 0.0))((t, line) =>
      (t._1 + 1, if (german.score(line) < english.score(line)) t._2 + 1 else t._2))

    println(statGer)
    println(statGer._2 / statGer._1)

    println(statEng)
    println(statEng._2 / statEng._1)

    val test = LanguageScorer.deserialize(new File(dir + "frequencies_en.dat"))

    val statTest = Source.fromFile(dir + "top10000en.txt").getLines()
        .filter(_.length > 3).foldLeft((0.0, 0.0))((t, line) =>
      (t._1 + 1, if (german.score(line) < test.score(line)) t._2 + 1 else t._2))

//    println(test.frequencyMap.filter { case (key, value) => english.frequencyMap.getOrElse(key, -1.0) != value })

    println(statTest)
    println(statTest._2 / statTest._1)
  }
}
