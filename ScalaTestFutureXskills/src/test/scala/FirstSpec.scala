import org.scalatest.flatspec.AnyFlatSpec

class FirstSpec extends AnyFlatSpec {
  behavior of "spark scala coding framework"
  it should "start with 'spark' " in {
    assert("spark scala coding framework".startsWith("spark"))
  }
  it should "end with 'framework' " in {
    assert("spark scala coding framework".endsWith("framework"))
  }

}
