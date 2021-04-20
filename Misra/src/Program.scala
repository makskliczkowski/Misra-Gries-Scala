import org.graalvm.compiler.nodes.InliningLog.Decision
import scala.StringBuilder
import scala.util.control._
import java.lang.NumberFormatException
import java.lang.Math
import scala.collection.mutable
import scala.collection.mutable
import java.nio.file.{Paths, Files}
import scala.io.Source
import scala.util.Random
import java.nio.file.Paths



object classes {
  val classes_set : Set[String] = Set("books", "cars", "employees")
  case class Book(name: String, authors : String, page_num : Int, average_rating : Double){
    def this() {
      this("Default", "Default", 0,0)
    }
  }
  case class Car(name: String, model : String ,year : Int, price : Int)
  {
    def this(){
      this("Default","Default", 100 , 1)
    }
  }
  case class Employee(name: String,job_role : String, MonthlyIncome : Double) {
    def this(){
      this("Default", "Default",1.0)
    }
    def whoAmI(): Unit ={
      val an_or_a = if (job_role.split(" ").length > 1) "" else if (Set('a','e','i','o', 'u').contains(job_role.charAt(0))) "an" else "a"
      println("My name is " + name + " and I am " + an_or_a + " " +  job_role + ".")
    }
  }


}

object methods {
  final case class CustomException(private val message: String = "",
                                   private val cause: Throwable = None.orNull)
    extends Exception(message, cause)
  // Others
  def read_csv[A](dir : String, which :String) : Set[A] ={
    if(!Files.exists(Paths.get(dir))) throw CustomException("Can't open file that isn't there\n")
    val src= Source.fromFile(name = dir)
    val index = src.bufferedReader.readLine.mkString.split(',')
    src.close()
    var elems_to_take = Map[String,Int]()
    var parsingSet :Set[String] = Set[String]()
    var aSet : Set[A] = Set[A]()
    //println(aSet.getClass)
    // check if user did not lie about the type
    which match {
      case "book" => parsingSet = Set("title", "authors","average_rating","num_pages")
      case "car" => parsingSet = Set("price","model","brand", "year")
      case "employee" => parsingSet = Set("Name", "LastName","JobRole","MonthlyIncome")
      case _ => throw CustomException("Don't have that type to parse it")
    }

    var temp : Int = 0
    for(i <- index){
      if(parsingSet.contains(i)) {
        elems_to_take += (i -> temp)
      }
      temp += 1
    }

    if(elems_to_take.keys.isEmpty) throw CustomException("Can't find interesting attributes there")
    var skip = 0
    val bufferedSource = Source.fromFile(name = dir)
    which match {
      case "book" =>
        if(elems_to_take.keys.size != 4) throw CustomException("Don't have enough columns")
        for(line <- bufferedSource.getLines) {
          if(skip ==0) skip += 1
          else {
            val list = line.mkString.split(',')
            //println(list.mkString)
            aSet += classes.Book(list(elems_to_take("title")), list(elems_to_take("authors")), list(elems_to_take("num_pages")).toInt, list(elems_to_take("average_rating")).toDouble).asInstanceOf[A]
          }
        }
      case "car" =>
        if(elems_to_take.keys.size != 4) throw CustomException("Don't have enough columns")
        for(line <- bufferedSource.getLines) {
          if(skip ==0) skip += 1
          else {
            val list = line.mkString.split(',')
            aSet += classes.Car(list(elems_to_take("brand")),list(elems_to_take("model")), list(elems_to_take("year")).toInt, list(elems_to_take("price")).toInt).asInstanceOf[A]
          }
        }
      case "employee" =>
        if(elems_to_take.keys.size != 4) throw CustomException("Don't have enough columns")
        for(line <-  bufferedSource.getLines) {
          if(skip ==0) skip += 1
          else {
            val list = line.mkString.split(',')
            aSet += classes.Employee(list(elems_to_take("Name")) + " "+ list(elems_to_take("LastName")), list(elems_to_take("JobRole")), list(elems_to_take("MonthlyIncome")).toDouble).asInstanceOf[A]
          }
        }
      case _ => throw CustomException("Don't have that type to parse it")
    }
    bufferedSource.close()
    aSet
  }
  def misra_gries[A](k:Int, stream : LazyList[A], how_many :Int) : Map[A,Int] ={
    var map = scala.collection.mutable.Map[A,Int]()
    var step = 0
    while(step <= how_many){
      val elem : A = stream(step)
      if(map.contains(elem)){
        map(elem) += 1
      }
      else if(map.size < k - 1){
        map += (elem -> 1)
      }
      else{
        for ((k,v) <- map){
          map(k) = v-1
          if(map(k) == 0){
            map  -= k
          }
        }
      }
      step = step + 1
    }
    map.toMap
  }
  // get random element from set
  def random[T](s: Set[T]): T = {
    val n = util.Random.nextInt(s.size)
    s.iterator.drop(n).next
  }
}






object Misra_Gries{
  val rd = new Random(System.currentTimeMillis)
  def main(args: Array[String]): Unit = {
    println(Paths.get(".").toAbsolutePath)
    val BookSet = methods.read_csv[classes.Book]("./Misra/books.csv","book")
    val CarSet = methods.read_csv[classes.Car]("./Misra/USA_cars_datasets.csv", "car")
    val EmployeeSet = methods.read_csv[classes.Employee]("./Misra/People2.csv", "employee")
    val BookSubset = BookSet.take(10)
    val CarSubset = CarSet.take(10)
    val EmployeeSubset = EmployeeSet.take(10)
    //println(CarSet)
    def intStream : LazyList[Int] = (rd.nextInt(10)).abs   #:: intStream
    def bookStream : LazyList[classes.Book] = methods.random[classes.Book](BookSet) #:: bookStream
    def carStream : scala.collection.immutable.LazyList[classes.Car] = methods.random[classes.Car](CarSet) #:: carStream
    def employeeStream : LazyList[classes.Employee] = methods.random(EmployeeSet) #:: employeeStream

    def bookSubstream : LazyList[classes.Book] = methods.random[classes.Book](BookSubset) #:: bookSubstream

    println("Printing book stream")
    bookStream take 20 foreach println
    print("\n\n")
    println("Printing car stream")
    carStream take 10 foreach println
    print("\n\n")
    println("Printing employee stream")
    employeeStream take 10 foreach println

    print("\n\n")
    println(methods.misra_gries[Int](11,intStream,10000))
    println(methods.misra_gries[classes.Book](7,bookSubstream,1000))
  }
}