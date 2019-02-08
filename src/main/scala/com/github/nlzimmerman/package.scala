package com.github

// I need to rethink my package structure.
package object nlzimmerman {
  /** These functions ARE used to manipulate the sample list in
    * GKRecord
    */
  // def listInsert[U](l: List[U], i: Int, a: U): List[U] = {
  //   (l.dropRight(l.length-i) :+ a) ::: l.drop(i)
  // }
  def seqInsert[U](l: Seq[U], i: Int, a: U): Seq[U] = {
    l.dropRight(l.length-i) ++ (a +: l.drop(i))
  }
  def seqReplace[U](l: Seq[U], i: Int, a: U): Seq[U] = {
    l.dropRight(l.length-i) ++ (a +: l.drop(i+1))
  }
  /** These functions are not used, but I used them when I was smoke-testing
    * the code and they might be useful in the future.
    */
  def writeStringsUTF8(in: Seq[String], filename: String): Unit = {
    // doing the imports here just to keep the namespace clean in this package.
    import java.io.{
      BufferedWriter,
      OutputStreamWriter,
      FileOutputStream
    }
    import java.nio.charset.Charset

    val writer: BufferedWriter = new BufferedWriter(
      new OutputStreamWriter(
        new FileOutputStream(filename),
        Charset.forName("UTF-8").newEncoder
      )
    )
    in.foreach(
      (x: String) => {
        writer.write(x)
        writer.write("\n")
      }
    )
    writer.close
  }
  def readStringsUTF8(filename: String): Seq[String] = {
    import java.io.{
      BufferedReader,
      InputStreamReader,
      FileInputStream
    }
    import java.nio.charset.Charset
    import scala.collection.mutable.ListBuffer

    val reader: BufferedReader = new BufferedReader(
      new InputStreamReader(
        new FileInputStream(filename),
        Charset.forName("UTF-8").newDecoder
      )
    )
    val input: Seq[String] = {
      var line: String = null
      val tmp: ListBuffer[String] = ListBuffer.empty[String]
      while ({
        line = reader.readLine
        line != null
      }) {
        tmp.append(line)
      }
      tmp.toSeq
    }
    reader.close
    input
  }

}
