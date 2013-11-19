package simulations

import org.scalatest.FunSuite

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.collection.immutable.IndexedSeq

@RunWith(classOf[JUnitRunner])
class CircuitSuite extends CircuitSimulator with FunSuite {
  val InverterDelay = 1
  val AndGateDelay = 3
  val OrGateDelay = 5

  test("andGate example") {
    val in1, in2, out = new Wire
    andGate(in1, in2, out)
    in1.setSignal(false)
    in2.setSignal(false)
    run

    assert(out.getSignal === false, "and 1")

    in1.setSignal(true)
    run

    assert(out.getSignal === false, "and 2")

    in2.setSignal(true)
    run

    assert(out.getSignal === true, "and 3")
  }

  def numberToControlSignalWithLength(n: Int, minDigits: Int) = {

    def numberToControlSignal(n: Int): List[Boolean] = {
      val evenOrOdd = n % 2
      val quotient = n / 2
      val recursion = if (quotient == 0) List.empty else numberToControlSignal(quotient)
      evenOrOdd match {
        case 0 => recursion ++ List(false)
        case 1 => recursion ++ List(true)
        case _ => throw new IllegalStateException()
      }
    }

    val minimumSignal = numberToControlSignal(n)
    List.fill(minDigits - minimumSignal.size)(false) ++ minimumSignal
  }


  def testDemux(numberOfBits: Int, controlSignalAsInt: Int) {

    val numberOfOuts = BigInt(2).pow(numberOfBits).toInt

    val inputSignal = new Wire
    val controlSignal = List.fill(numberOfBits)(new Wire)
    val outSignal = List.fill(numberOfOuts)(new Wire)

    // logging
    //    probe("IN", inputSignal)
    //    controlSignal.zipWithIndex.foreach { case (w,i) => probe(s"CTRL-$i", w) }
    //    outSignal.zipWithIndex.foreach { case (w,i) => probe(s"OUT-$i", w) }

    // set up the Demux
    demux(inputSignal, controlSignal, outSignal)

    // set all control signals expecting the lengths of the lists to be equal
    val controlValues = numberToControlSignalWithLength(controlSignalAsInt, numberOfBits)
    controlSignal.zip(controlValues).foreach {
      case (w, b) => w.setSignal(b)
    }

    // set input to false and run
    inputSignal.setSignal(false)
    run

    println("INPUT: " + inputSignal.getSignal)
    println("CONTROL: " + controlSignal.map(_.getSignal))
    println("OUT: " + outSignal.map(_.getSignal))

    // expect all the signals to be false
    outSignal.zipWithIndex.foreach {
      case (wire, index) => {
        val expectedSignal = false
        assert(wire.getSignal === expectedSignal, s"Signal at index=$index should be $expectedSignal")
      }
    }

    // set input to true and run
    inputSignal.setSignal(true)
    run

    println("INPUT: " + inputSignal.getSignal)
    println("CONTROL: " + controlSignal.map(_.getSignal))
    println("OUT: " + outSignal.map(_.getSignal))

    // expect the controlSignalAsInt's signal to be true
    outSignal.reverse.zipWithIndex.foreach {
      case (wire, index) => {
        val expectedSignal = index == controlSignalAsInt
        assert(wire.getSignal === expectedSignal, s"Signal at index=$index should be $expectedSignal")
      }
    }

  }

  test("Demux") {
    testDemux(4, 8)
    testDemux(8, 3)
    testDemux(0, 0)
    testDemux(1, 0)
    testDemux(3, 2)
    testDemux(1, 1)
  }

}
