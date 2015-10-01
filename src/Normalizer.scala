/**
 * Created by Zalan on 9/30/2015.
 */
object Normalizer {

  def normalize(text: String): String = { 
      text.replaceAll("[^a-zA-Z \t\n\r\f]", "").toLowerCase()
  }

}
