package com.example.commons.collection

object ListImplicits {

  implicit class ListHelper(val x: List[Any]) {
    def mapToType[T] = x.map(_.asInstanceOf[T])

  }
}
