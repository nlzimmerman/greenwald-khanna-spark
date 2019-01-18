package com.github

// I need to rethink my package structure.
package object nlzimmerman {
  def listInsert[U](l: List[U], i: Int, a: U): List[U] = {
    (l.dropRight(l.length-i) :+ a) ::: l.drop(i)
  }
  def listReplace[U](l: List[U], i: Int, a: U): List[U] = {
    (l.dropRight(l.length-i) :+ a) ::: l.drop(i+1)
  }
}
