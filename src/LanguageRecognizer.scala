import java.io.InputStream
import java.util.Locale

class LanguageRecognizer(scorers: Seq[LanguageScorer]) {

  require(scorers.nonEmpty)

  def recognize(string: String): Locale = {
    scorers.maxBy(_.score(string)).language
  }
}

object LanguageRecognizer {

  def fromInputStreams(files: Seq[InputStream]): LanguageRecognizer = {
    new LanguageRecognizer(files.map(LanguageScorer.deserialize))
  }
}
