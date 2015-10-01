/**
 * Created by Zalan on 9/30/2015.
 */
object SimHash {

  def getCodeOfShingle(shingle: List[String]): Long = {
    var h = 1125899906842597L
    var sh = shingle.mkString
    for (i <- 0 to sh.length - 1) {
      h = h * 31 + sh.charAt(i)
    }
    h
  }

  def getCodeOfDocument(doc: String, shingleSize: Int): Long = {
    var words = Tokenizer.tokenize(doc)
    val values = Array.fill[Int](64)(0)
    for (shingle <- words.sliding(shingleSize)) {
      val code = getCodeOfShingle(shingle)
      for (i <- 0 to 63) {
        if ( (code & (2 << i)) > 0) {
          values(i) = values(i) + 1
        }
        else {
          values(i) = values(i) - 1
        }
      }
    }
    values.map(x => if (x >= 0) 1 else 0).reduceLeft(_*2+_)
  }

  def compareCodes(code1: Long, code2: Long): Long = {
    64 - java.lang.Long.bitCount(code1 ^ code2)
  }

  def similarity(s1: String, s2: String, shingleSize: Int): Double = {
    compareCodes(getCodeOfDocument(s1, shingleSize), getCodeOfDocument(s2, shingleSize)) / 64.0
  }

  def main(args: Array[String]) {

    println(similarity("alma korte szilva", "alma korte szilva cseresznye", 3))


  }
}
