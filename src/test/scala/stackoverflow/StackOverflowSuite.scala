package stackoverflow

import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import stackoverflow.StackOverflow.{kmeans, sampleVectors, scoredPostings, vectorPostings, findClosest, averageVectors}

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterEach {


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  var sparkSession: SparkSession = _

  override def beforeEach(): Unit = {
    sparkSession = SparkSession.builder().appName("test").master("local").config("", "").getOrCreate();
  }

  test("test transformation"){
    val lines   = sparkSession.sparkContext.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
//    println("size of the initial RDD: - " + lines.count())
    val raw = StackOverflow.rawPostings(lines)
//    println("size of raw positings: - " + raw.count())
    val questions = raw.filter(p => p.postingType == 1).map(p => (p.id, p))
//    println("Questions")
//    questions.take(10).foreach(println)
    val answers = raw.filter(p => p.postingType == 2).map(p => (p.parentId.get, p))
//    println("Answers")
//    answers.take(10).foreach(println)
//    val grouped = questions.leftOuterJoin(answers).groupByKey()
    val grouped = questions.join(answers).groupByKey()
//    println("Grouped")
//    grouped.take(10).foreach(println)


//    questions.join(answers).groupByKey()
//    (9182034,CompactBuffer(
//      (Posting(1,9182034,None,None,20,Some(C#)),Posting(2,9182653,None,Some(9182034),14,None)),
//    (Posting(1,9182034,None,None,20,Some(C#)),Posting(2,9183174,None,Some(9182034),2,None))))


//    questions.join(answers)
//    (18583638,(Posting(1,18583638,None,None,-1,Some(Objective-C)),Posting(2,18583768,None,Some(18583638),1,None)))
//    (10423506,(Posting(1,10423506,None,None,0,Some(PHP)),Posting(2,10424789,None,Some(10423506),0,None)))
//    (9182034,(Posting(1,9182034,None,None,20,Some(C#)),Posting(2,9182653,None,Some(9182034),14,None)))
//    (9182034,(Posting(1,9182034,None,None,20,Some(C#)),Posting(2,9183174,None,Some(9182034),2,None)))
//    (8977422,(Posting(1,8977422,None,None,1,Some(C#)),Posting(2,8978009,None,Some(8977422),3,None)))
//    (8977422,(Posting(1,8977422,None,None,1,Some(C#)),Posting(2,8978041,None,Some(8977422),4,None)))
//    (8140350,(Posting(1,8140350,None,None,2,Some(C#)),Posting(2,8140693,None,Some(8140350),1,None)))
//    (8140350,(Posting(1,8140350,None,None,2,Some(C#)),Posting(2,8140808,None,Some(8140350),2,None)))
//    (8140350,(Posting(1,8140350,None,None,2,Some(C#)),Posting(2,27808217,None,Some(8140350),0,None)))
//    (2157804,(Posting(1,2157804,None,None,2,Some(PHP)),Posting(2,2157825,None,Some(2157804),4,None)))


//    leftOuterJoin(answers).groupByKey()
//    (2157804,CompactBuffer(
//      (Posting(1,2157804,None,None,2,Some(PHP)),Some(Posting(2,2157825,None,Some(2157804),4,None))),
//      (Posting(1,2157804,None,None,2,Some(PHP)),Some(Posting(2,2157830,None,Some(2157804),2,None))),
//      (Posting(1,2157804,None,None,2,Some(PHP)),Some(Posting(2,2157831,None,Some(2157804),2,None))),
//      (Posting(1,2157804,None,None,2,Some(PHP)),Some(Posting(2,2157836,None,Some(2157804),0,None))))
//    )
    val scored  = scoredPostings(grouped)
//    scored.take(10).foreach(println)

//    (Posting(1,17743038,None,None,1,Some(Python)),0)
//    (Posting(1,18583638,None,None,-1,Some(Objective-C)),1)
//    (Posting(1,10423506,None,None,0,Some(PHP)),0)
//    (Posting(1,13882656,None,None,-1,Some(PHP)),5)
//    (Posting(1,26816820,None,None,0,Some(CSS)),0)
//    (Posting(1,9182034,None,None,20,Some(C#)),14)
//    (Posting(1,8977422,None,None,1,Some(C#)),4)
//    (Posting(1,8732148,None,None,0,Some(Python)),3)
//    (Posting(1,27993726,None,None,1,Some(Java)),1)
//    (Posting(1,182316,None,None,22,Some(Java)),25)

    val vectors = vectorPostings(scored)
    assert(vectors.count() == 2121822, "Incorrect number of vectors: " + vectors.count())

//    val means   = kmeans(sampleVectors(vectors), vectors, debug = true)
    val sample = sampleVectors(vectors)
//    sample.foreach(println)

//    (450000,1)
//    (450000,1)
//    (450000,3)
//    (0,0)
//    (0,3)
//    (0,1)
//    (600000,2)
//    (600000,1)
//    (600000,0)
//    (150000,4)
//    (150000,1)
//    (150000,3)


    val cluster = vectors.map(p => (findClosest(p, sample), p))
//    cluster.take(10).foreach(println)

//    (10,(150000,0))
//    (33,(400000,1))
//    (32,(100000,0))
//    (31,(100000,5))
//    (25,(350000,0))
//    (20,(200000,14))
//    (20,(200000,4))
//    (11,(150000,3))
//    (15,(50000,1))
//    (15,(50000,25))

    val mean = cluster.groupByKey().filter(_._1 == 1).map{case (index: Int, ps: Iterable[(Int, Int)]) => (index, averageVectors(ps))}
    println(mean)
//    val newMeans = cluster.groupByKey().map{case (index: Int, ps: Iterable[(Int, Int)]) => (index, averageVectors(ps))}
//    println(newMeans.count())
//    println()
//    newMeans.foreach(println)
//    missing in new means 1, 16,
//    val newMeans: Array[(Int, Int)] = cluster.groupByKey().map{case (index: Int, ps: Iterable[(Int, Int)]) => averageVectors(ps)}.collect()
//    sample.foreach(println)
//    println()
//    newMeans.foreach(println)
//    newMeans.take(10).foreach(println)

//    val means   = kmeans(sampleVectors(vectors), vectors, debug = true)

//    val closest = vectors.map(p => (findClosest(p, means), p))
//    val closestGrouped = closest.groupByKey()

//    val langs = closestGrouped.map{case (index: Int, ps: Iterable[(Int, Int)]) => (index, ps.size)}
//    langs.foreach(println)

//    val median = closestGrouped.mapValues { vs =>
//      // most common language in the cluster
//      val langLabel: String   = ???
//      // percent of the questions in the most common language
//      val langPercent: Double = ???
//      val clusterSize: Int    = ???
//      val medianScore: Int    = ???
//
//      (langLabel, langPercent, clusterSize, medianScore)
//    }


  }

//    test("clusterResults"){
//      val centers = Array((0,0), (100000, 0))
//      val rdd = sparkSession.sparkContext.parallelize(List(
//        (0, 1000),
//        (0, 23),
//        (0, 234),
//        (0, 0),
//        (0, 1),
//        (0, 1),
//        (50000, 2),
//        (50000, 10),
//        (100000, 2),
//        (100000, 5),
//        (100000, 10),
//        (200000, 100)  ))
//      testObject.printResults(testObject.clusterResults(centers, rdd))
//    }


//  test("testObject can be instantiated") {
//    val instantiatable = try {
//      testObject
//      true
//    } catch {
//      case _: Throwable => false
//    }
//    assert(instantiatable, "Can't instantiate a StackOverflow object")
//  }


}
