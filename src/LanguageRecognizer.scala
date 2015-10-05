import java.io.File
import java.util.Locale

class LanguageRecognizer(scorers: Seq[LanguageScorer]) {

  require(scorers.nonEmpty)

  def recognize(string: String): Locale = {
    scorers.maxBy(_.score(string)).language
  }
}

object LanguageRecognizer {

  def fromFiles(files: Seq[File]): LanguageRecognizer = {
    new LanguageRecognizer(files.map(LanguageScorer.deserialize))
  }
}
