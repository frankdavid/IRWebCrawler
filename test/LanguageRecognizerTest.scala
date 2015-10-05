import java.io.File

import scala.io.Source

object LanguageRecognizerTest {

  val dir = "data/lang/"

  def main(args: Array[String]): Unit = {
    val dir = "data/lang/"

    val recognizer = LanguageRecognizer.fromFiles(Seq(
      new File(dir + "frequencies_en.dat"),
      new File(dir + "frequencies_de.dat")))

    val file = Source.fromFile(dir + "sentences.txt")

    file.getLines().foreach { line =>
      val lang = recognizer.recognize(line)
      println(s"$lang: $line")
    }

    file.close()
  }
}
