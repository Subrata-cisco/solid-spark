package com.subrata.spark

object NineComplement {
  
  def main(args: Array[String]) {
    
    val number = "1234567890";
    val complArray = for(num <- number.toCharArray()) yield {
      val compl  = (('9').toInt  -  num.toInt + '0'.toInt).toChar; 
      compl
    }
    
    var changedArray : Array[Char] = new Array[Char](number.length())
    for ((e, count) <- complArray.zipWithIndex) {
        if(count == 0 || count == 1 || count == 8 || count == 9){
          changedArray(9-count) = e
        }else if(count == 2){
          changedArray(2) = complArray(4)
          changedArray(3) = complArray(3)
          changedArray(4) = e
        }else if(count == 5){
          changedArray(5) = complArray(7)
          changedArray(6) = complArray(6)
          changedArray(7) = e
        }
    }
    
    println(changedArray.mkString)
    
  }

  
}