object Normalizer {

  def normalize(text: String): String = { 
      text.replaceAll("[^a-zA-Z \t\n\r\f]", "").toLowerCase()
  }
}
