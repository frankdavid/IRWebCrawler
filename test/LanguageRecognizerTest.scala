import scala.io.Source

object LanguageRecognizerTest {

  def main(args: Array[String]): Unit = {
    val recognizer = LanguageRecognizer.fromInputStreams(Seq(
      FileLoader.loadFileFromPathOrJar("data/frequencies_en.dat"),
      FileLoader.loadFileFromPathOrJar("data/frequencies_de.dat")))

    val file = Source.fromInputStream(FileLoader.loadFileFromPathOrJar("data/sentences.txt"), "UTF-8")

    file.getLines().foreach { line =>
      val lang = recognizer.recognize(line)
      println(s"$lang: $line")
    }

    file.close()
  }
}
