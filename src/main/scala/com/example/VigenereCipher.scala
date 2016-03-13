package com.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


object VigenereCipher {

  val conf = new SparkConf().setMaster("local").setAppName("App")
  val sc = new SparkContext(conf)
  val ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".toVector

  def encryptData(text: RDD[Char], key: String): Unit = {

    val arrayKey = key.toList
    val lenKey = arrayKey.length
    var counter = 0

    for (letter <- text) {
      val alphaLetter = ALPHABET.indexOf(letter)
      val keyLetter = ALPHABET.indexOf(arrayKey(counter % lenKey))
      val encryptLetterNumber = (alphaLetter + keyLetter) % ALPHABET.length
      println(letter + " " + ALPHABET(encryptLetterNumber))
      counter = counter + 1
    }

  }
  
  def decryptData(text: RDD[Char], key: String): RDD[Char] = {

    val arrayKey = key.toList
    val lenKey = arrayKey.length
    var counter = 0
    val textLen = text.count().toInt
    val textFinal:Array[Char] = text.collect()

    var message = scala.collection.mutable.ListBuffer[Char]()

    for (letter <- 0 to textLen-1){

      val alphaLetter = ALPHABET.indexOf(textFinal(letter))
      val keyLetter = ALPHABET.indexOf(arrayKey(counter % lenKey))
      val encryptLetterNumber = (ALPHABET.length + (alphaLetter - keyLetter)) % ALPHABET.length
      message += ALPHABET(encryptLetterNumber)
      //      println(letter + " " + ALPHABET(encryptLetterNumber))
      counter = counter + 1

    }

    val RDDdecryptMessage = sc.parallelize(message)
    RDDdecryptMessage
  }

  //Get the password length of a ciphered text
  def passLen(text: RDD[String]): Int ={

    val patternWords = text
      .flatMap(_.replace(" ","").sliding(3,3))
      .map(_.toUpperCase).collect().toList

    val text2Message= text.collect()

    var distances = scala.collection.mutable.ListBuffer[Int]()

    for (x<-patternWords){

      val fOcurrence = text2Message(0).indexOf(x,0)

      if (text2Message(0).indexOf(x,fOcurrence+1)>0){

        val element=text2Message(0).indexOf(x,fOcurrence+1)-text2Message(0).indexOf(x,0)

        distances += element

      }

    }

    val finalDistances = distances.toSet.toVector

    var ans:Int = finalDistances(0)

    for(x<- 0 to finalDistances.size-1){

      ans = gcd(ans,finalDistances(x))

    }
    ans
  }

  //Get the greatest common divisor of two numbers
  def gcd(x: Int, y: Int): Int = {
    var a = x
    var b = y
    while (a != 0) {
      val temp = a
      a = b % a
      b = temp
    }
    b
  }

  //Get the first 4  with more occurences in the text
  def frecuencyAnalysis(text: RDD[Char]): List[Char] = {

    val countPerWord = text.filter(ALPHABET.toSet contains _)
      .map(character => (character, 1))
      .reduceByKey(_ + _)
      .sortBy(- _._2)
      .map(_._1)
      .take(4).toList

    countPerWord

  }

  //Check if the 4 most common words in the List are similar with the most common words in English
  def checkFrecuencyWeight(frecuencyList: List[Char]): Int ={

    var weight=0

    for(x <-  frecuencyList){

      if(x == 'E') weight += 1
      if(x == 'T') weight += 1
      if(x == 'A') weight += 1
      if(x == 'O') weight += 1

    }

    weight
  }

  //Get all the characters of the key
  def kasiskiAttack(text:RDD[Char],keyLen:Int): scala.collection.mutable.ListBuffer[scala.collection.mutable.ListBuffer[Char]] ={

    var keysSet = scala.collection.mutable.ListBuffer[scala.collection.mutable.ListBuffer[Char]]()

    for (x<-0 to keyLen-1){

      val pieceText = text.zipWithIndex()
        .map(x=>x)
        .filter(_._2>=x)
        .map(x=>x._1)

      val finalPiece = pieceText.zipWithIndex().filter(_._2%keyLen==0).map(_._1)

      var possibleKey= scala.collection.mutable.ListBuffer[Char]()

      for(x <- ALPHABET){

       if (checkFrecuencyWeight(frecuencyAnalysis(decryptData(finalPiece,x.toString)))>1){
         possibleKey += x
       }

      }

      keysSet += possibleKey
    }
    keysSet
  }

  def main(args: Array[String]) {

//    val input = sc.textFile("test.log")
//
//    val words = input.flatMap(_.split(" "))
//      .map(_.toUpperCase)
//      .flatMap(_.toList)
//
//    val keyCipher = "WORLD"
//
//    encryptData(words, keyCipher)

    val input2 = sc.textFile("testEncrypt.log")

    val words2 = input2.flatMap(_.split(" "))
      .map(_.toUpperCase)
      .flatMap(_.toList)

//    decryptData(words2, keyCipher)

    println("--Getting data from Kasiski Attack--")

    val keyLen = passLen(input2)

    println("Pass lenght: " + keyLen)

    val test = kasiskiAttack(words2,passLen(input2))

    println("Possible chars of the key for decrypt message: ")

    test.foreach(println(_))

  }

}