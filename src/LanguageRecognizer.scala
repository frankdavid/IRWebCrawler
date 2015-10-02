import java.io.File
import java.util.Locale

class LanguageRecognizer(scorers: Map[Locale, LanguageScorer]) {
  def recognize(string: String) = {
    scorers.maxBy {scorer => scorer._2.score(string)}._1
  }
}

object LanguageRecognizer {
  def fromFiles(files: Map[Locale, File]) = {
    files.mapValues(LanguageScorer.readFromFile)
  }
}
