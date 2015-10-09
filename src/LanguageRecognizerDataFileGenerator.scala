import java.io.File
import java.util.Locale

object LanguageRecognizerDataFileGenerator {

  def main(args: Array[String]) {
    LanguageScorer.trainFromText(Locale.ENGLISH, FileLoader.loadFileFromPathOrJar("data/1.en"), 3)
        .writeToFile(new File("data/frequencies_en.dat"))

    LanguageScorer.trainFromText(Locale.GERMAN, FileLoader.loadFileFromPathOrJar("data/1.de"), 3)
        .writeToFile(new File("data/frequencies_de.dat"))
  }
}
