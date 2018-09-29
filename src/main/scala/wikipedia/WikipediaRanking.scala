package wikipedia

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

case class WikipediaArticle(title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
}

object WikipediaRanking {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("scala-spark-big-data")
  val sc: SparkContext = new SparkContext(config = conf)
  // Hint: use a combination of `sc.textFile`, `WikipediaData.filePath` and `WikipediaData.parse`
  val wikiRdd: RDD[WikipediaArticle] = sc.textFile(WikipediaData.filePath).map(s => WikipediaData.parse(s))

  /** Returns the number of articles on which the language `lang` occurs.
   *  Hint1: consider using method `aggregate` on RDD[T].
   *  Hint2: consider using method `mentionsLanguage` on `WikipediaArticle`
   */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = rdd.aggregate(0)(
      (acc, w) => acc + (if(w.mentionsLanguage(lang)) 1 else 0),
      (acc1, acc2) => acc1 + acc2
  )
  /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  implicit val sortIntegersReverse = new Ordering[Int] {
    override def compare(x: Int, y: Int): Int = y.compare(x)
  }
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = langs
    .filter(lang => if(occurrencesOfLang(lang, rdd) > 0) true else false)
    .map(lang => (lang, occurrencesOfLang(lang, rdd)))
    .sortBy(_._2)(sortIntegersReverse)


  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */

  private def textContains(needle: String, haystack: String): Boolean = haystack.split(" ").contains(needle)

  private def findLanguages(langs: List[String], article: WikipediaArticle) =
    langs
      .filter(textContains(_, article.text))

  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] =
    rdd.flatMap(article => {
      findLanguages(langs, article)
        .map(lang => (lang, article))
    }).groupByKey
//    val index: List[String, RDD[WikipediaArticle]] = langs.map()
//      for(lang <- langs) yield (lang, rdd.filter(w => w.mentionsLanguage(lang)))
//    val index = langs.map(lang => (lang, rdd.filter(w => w.mentionsLanguage(lang)).collect().toIterable))
//    sc.parallelize(index)

  /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
//  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]) = {
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] =
    index.mapValues(it => it.size).sortBy(_._2).collect().toList

//    (for((lang, it) <- index)
//      yield (lang, it.count(w => w.mentionsLanguage(lang)))
//    ).collect().toList

  /* (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] =
    langs.flatMap(lang => {
      val rdd1 = rdd.filter(w => w.mentionsLanguage(lang)).map(w => (lang, 1))
      rdd1.reduceByKey(_ + _).collect().toList
    }).sortBy(_._2)


  def main(args: Array[String]) {

    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
    println(timing)
    sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
