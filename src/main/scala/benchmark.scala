object benchmark {

  private val startTimes = collection.mutable.Map[String, Long]()
  private val durations = collection.mutable.Map[String, Long]()
  
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
  def print = values foreach println

}
