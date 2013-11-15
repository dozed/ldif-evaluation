
object benchmark {

  private val startTimes = collection.mutable.Map[String, Long]()
  private val durations = collection.mutable.Map[String, Long]()

  def run[T](l: String)(code: => T) = {
    up(l)
    val res = code
    down(l)
    res
  }

  def run[T](code: => T) = {
    up("default")
    val res = code
    down("default")
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
    values foreach println
  }

}
