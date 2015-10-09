import java.io.{FileInputStream, File, InputStream}

object FileLoader {

  /**
   * Open a file from the current directory. If not found, try to load from the jar.
   * @return input stream, or null if the filePath can be found neither in the current dir, nor in the jar
   */
  def loadFileFromPathOrJar(filePath: String): InputStream = {
    val file = new File(filePath)
    if (file.exists()) {
      new FileInputStream(file)
    } else {
      getClass.getResourceAsStream(filePath)
    }
  }
}
