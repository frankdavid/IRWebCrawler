import java.io.File
import java.util.Locale

object LanguageRecognizerDataFileGenerator {

  def main(args: Array[String]) {
    val dir = "data/lang/"

    LanguageScorer.trainFromText(Locale.ENGLISH, new File(dir + "1.en"), 3)
        .writeToFile(new File("frequencies_en.dat"))

    LanguageScorer.trainFromText(Locale.GERMAN, new File(dir + "1.de"), 3)
        .writeToFile(new File("frequencies_de.dat"))
  }
}
