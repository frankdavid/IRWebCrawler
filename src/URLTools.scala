/**
 * Created by Zalan on 10/1/2015.
 */
object URLTools {

  def main(args: Array[String]){
    import scala.io.Source
    val html = Source.fromURL("http://idvm-infk-hofmann03.inf.ethz.ch/eth/www.ethz.ch/en.html")
    val s = html.mkString
    println(s.replaceAll("(?s)<[^>]*>(\\s*<[^>]*>)*", " "))
  }
}
