package com.github
import scala.annotation.tailrec
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
  private[nlzimmerman] def makeBands(maxDelta: Long) = {
    @tailrec
    def makeBandsInner(
      delta: Long,
      currentBand: Long=0,
      acc: List[Long] = List.empty[Long]
    ): List[Long] = {
      // remember that 1 << x == math.pow(2,x)
      val nextThreshold: Long = maxDelta - (1L << currentBand) - (maxDelta % (1L << currentBand))
      /** I don't know whether this is more readable than an if/else if/else
        * or not. I don't think there's anything wrong with this but historically
        * I have used if/elses in situations like this.
        */
      delta match {
        /** delta is decrementing from the highest possible value to 0
          * Once it hits 0, we are done.
          */
        case 0L => (currentBand + 1) +: acc
        /** since the variable name is lower case, I need backticks to make a
          * stable identifier.
          * https://www.scala-lang.org/files/archive/spec/2.11/08-pattern-matching.html
          */
        /** If delta == maxDelta, we just started, in this special case, prepend
          * a 0 to the (empty) list, not a 1
          */
        case `maxDelta` => makeBandsInner(
          delta-1,
          currentBand+1,
          0L +: acc
        )
        /** if we crossed a threshold, increment the band (including at this record)
          */
        case `nextThreshold` => makeBandsInner(
          delta-1,
          currentBand+1,
          (currentBand+1) +: acc
        )
        /** we didn't cross a threshold, so prepend the existing band and
          * keep going.
          */
        case _ => makeBandsInner(
          delta-1,
          currentBand,
          currentBand +: acc
        )
      }
    }
    makeBandsInner(maxDelta)
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
