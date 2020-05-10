import org.junit.Assert.assertEquals

class FuturexUtilTest {

  @org.junit.Test
  def test1(): Unit = {
    var exepected : Int = 10
    var actual : Int = FutureXUtil.divideVars(30,3)
    assertEquals(exepected , actual )
  }
  @org.junit.Test
  def test2(): Unit = {
    var exepected : Int = 15
    var actual : Int = FutureXUtil.divideVars(30,2)
    assertEquals(exepected , actual )
  }
}
