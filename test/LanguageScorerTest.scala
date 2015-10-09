import java.util.Locale

import scala.io.Source

object LanguageScorerTest {

  def main(args: Array[String]) {
    val english = LanguageScorer.trainFromText(Locale.ENGLISH, FileLoader.loadFileFromPathOrJar("data/1.en"), 3)

    val german = LanguageScorer.trainFromText(Locale.GERMAN, FileLoader.loadFileFromPathOrJar("data/1.de"), 3)

    val statGer = Source.fromInputStream(FileLoader.loadFileFromPathOrJar("data/top10000de.txt"), "UTF-8").getLines()
        .filter(_.length > 3).foldLeft((0.0, 0.0))((t, line) =>
      (t._1 + 1, if (german.score(line) > english.score(line)) t._2 + 1 else t._2))

    val statEng = Source.fromInputStream(FileLoader.loadFileFromPathOrJar("data/top10000en.txt"), "UTF-8").getLines()
        .filter(_.length > 3).foldLeft((0.0, 0.0))((t, line) =>
      (t._1 + 1, if (german.score(line) < english.score(line)) t._2 + 1 else t._2))

    println(statGer)
    println(statGer._2 / statGer._1)

    println(statEng)
    println(statEng._2 / statEng._1)

    val test = LanguageScorer.deserialize(FileLoader.loadFileFromPathOrJar("data/frequencies_en.dat"))

    val statTest = Source.fromInputStream(FileLoader.loadFileFromPathOrJar("data/top10000en.txt"), "UTF-8").getLines()
        .filter(_.length > 3).foldLeft((0.0, 0.0))((t, line) =>
      (t._1 + 1, if (german.score(line) < test.score(line)) t._2 + 1 else t._2))

//    println(test.frequencyMap.filter { case (key, value) => english.frequencyMap.getOrElse(key, -1.0) != value })

    println(statTest)
    println(statTest._2 / statTest._1)
  }
}
