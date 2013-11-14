
object benchmark {

  private val startTimes = collection.mutable.Map[String, Long]()
  private val durations = collection.mutable.Map[String, Long]()

  private var c = 0

  def run[T](l: String)(code: => T) = {
    up(l)
    val res = code
    down(l)
    c = c + 1
    if (c % 3 == 0) stats
    res
  }

  def up(l: String) = {
    startTimes(l) = System.currentTimeMillis()
  }

  def down(l: String) = {
    val t = System.currentTimeMillis()
    if (startTimes.contains(l)) {
      val s = startTimes(l)
      durations(l) = durations.getOrElse(l, 0L) + (t - s)
    }
  }

  def values = durations.toMap
  def stats = {
    println("-----------------------------------------------------")
    println(f"stats: $c")
    values foreach println
  }

}
