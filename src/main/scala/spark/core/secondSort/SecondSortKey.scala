package spark.core.secondSort

class SecondSortKey(val first: Int, val second: Int) extends Ordered[SecondSortKey] with Serializable {
  override def compare(that: SecondSortKey): Int = {
    if (first != that.first) {
      first - that.first
    } else {
      second - that.second
    }
  }
}
