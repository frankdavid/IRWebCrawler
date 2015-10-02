/**
 * Created by Zalan on 9/30/2015.
 */
object SimHash128 {

  var md=java.security.MessageDigest.getInstance("MD5")

  def getCodeOfShingle(shingle: List[String]): Array[Byte] = {
    md.digest(shingle.mkString.getBytes("UTF-8"))
  }

  def getCodeOfDocument(doc: String, shingleSize: Int): Array[Boolean] = {
//    var words = Tokenizer.tokenize(doc)
    val values = Array.fill[Int](128)(0)
    val docSplit = Tokenizer.tokenize(doc)
    for (shingle <- docSplit.sliding(shingleSize)) {
      val code = getCodeOfShingle(shingle)
      for (i <- 0 to 127) {
        if ( (code(i>>3) & (i&7)) > 0) {
          values(i) = values(i) + 1
        }
        else {
          values(i) = values(i) - 1
        }
      }
    }
    values.map(x => if (x >= 0) true else false)
  }

  def compareCodes(code1: Array[Boolean], code2: Array[Boolean]): Int = {
    (code1 zip code2).count{case(x, y) => x==y}
  }

  def similarity(s1: String, s2: String, shingleSize: Int): Double = {
    compareCodes(getCodeOfDocument(s1, shingleSize), getCodeOfDocument(s2, shingleSize)) / 128.0
  }

  def main(args: Array[String]) {

    println(similarity("Singapore may hold the dubious title of “most expensive city in the world,” but it remains the most popular place for expats to live and work, according to an annual survey of expats released by HSBC.", "Singapore may hold the dubious title of “most expensive city in the world,” but       dfgsdfg sdgbsd bfd sb bsdb vs bv   it remains the most popular place for expats to live and work, eccording to an annual survey of expats released by HSBC.", 5))


  }
}
