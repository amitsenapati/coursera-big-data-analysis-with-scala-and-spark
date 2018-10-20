import org.apache.spark.sql.functions.col

val otherList = List("t02","t18")
val columnNames = List("t021","t022","t03", "t18011", "t18012", "t18022")
val ignoreList = List("t1801", "t1803", "t1805")

val leisure = for{
  name <- columnNames
  if otherList.exists(a => name.startsWith(a)) && !ignoreList.exists(p => name.startsWith(p))
} yield col(name)