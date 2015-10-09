import scala.collection.immutable.BitSet

object SimHash128 {

  // note, that this is a method instead of a field, ie. it returns a new instance for every call, which is required
  // since MessageDigest is not thread safe
  private def md = java.security.MessageDigest.getInstance("MD5")

  private def getCodeOfShingle(shingle: List[String]): Array[Byte] = {
    md.digest(shingle.mkString.getBytes("UTF-8"))
  }

  def getCodeOfDocument(doc: String, shingleSize: Int = 5): BitSet = {
    val values = Array.fill[Int](128)(0)
    val docSplit = Tokenizer.tokenize(doc)
    for (shingle <- docSplit.sliding(shingleSize)) {
      val code = getCodeOfShingle(shingle)
      for (i <- 0 to 127) {
        if ((code(i >> 3) & (i & 7)) > 0) {
          values(i) = values(i) + 1
        }
        else {
          values(i) = values(i) - 1
        }
      }
    }

    BitSet() ++ values.indices.filter(idx => values(idx) >= 0)
  }

  def compareCodes(code1: BitSet, code2: BitSet): Int = {
    128 - hammingDistance(code1, code2)
  }

  def hammingDistance(code1: BitSet, code2: BitSet): Int = {
    (code1 ^ code2).size
  }

  def similarity(s1: String, s2: String, shingleSize: Int = 5): Double = {
    compareCodes(getCodeOfDocument(s1, shingleSize), getCodeOfDocument(s2, shingleSize)) / 128.0
  }

  def main(args: Array[String]) {
    println(similarity(
      "Singapore may hold the dubious title of “most expensive city in the world,” but it remains the most popular place for expats to live and work, according to an annual survey of expats released by HSBC.",
      "Singapore may hold the dubious title of “most expensive city in the world,” but       dfgsdfg sdgbsd bfd sb bsdb vs bv   it remains the most popular place for expats to live and work, eccording to an annual survey of expats released by HSBC.", 5))
  }
}
