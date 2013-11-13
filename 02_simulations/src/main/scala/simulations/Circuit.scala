package simulations

import common._

class Wire {
  private var sigVal = false
  private var actions: List[Simulator#Action] = List()

  def getSignal: Boolean = sigVal

  def setSignal(s: Boolean) {
    if (s != sigVal) {
      sigVal = s
      actions.foreach(action => action())
    }
  }

  def addAction(a: Simulator#Action) {
    actions = a :: actions
    a()
  }
}

abstract class CircuitSimulator extends Simulator {

  val InverterDelay: Int
  val AndGateDelay: Int
  val OrGateDelay: Int

  def probe(name: String, wire: Wire) {
    wire addAction {
      () => afterDelay(0) {
        println(
          "  " + currentTime + ": " + name + " -> " + wire.getSignal)
      }
    }
  }

  def inverter(input: Wire, output: Wire) {
    def invertAction() {
      val inputSig = input.getSignal
      afterDelay(InverterDelay) {
        output.setSignal(!inputSig)
      }
    }
    input addAction invertAction
  }

  def andGate(a1: Wire, a2: Wire, output: Wire) {
    def andAction() {
      val a1Sig = a1.getSignal
      val a2Sig = a2.getSignal
      afterDelay(AndGateDelay) {
        output.setSignal(a1Sig & a2Sig)
      }
    }
    a1 addAction andAction
    a2 addAction andAction
  }

  //
  // to complete with orGates and demux...
  //

  def orGate(a1: Wire, a2: Wire, output: Wire) {
    def orAction() {
      val a1Sig = a1.getSignal
      val a2Sig = a2.getSignal
      afterDelay(OrGateDelay) {
        output.setSignal(a1Sig | a2Sig)
      }
    }
    a1 addAction orAction
    a2 addAction orAction
  }

  def orGate2(a1: Wire, a2: Wire, output: Wire) {
    val notA1 = new Wire
    val notA2 = new Wire

    inverter(a1, notA1)
    inverter(a2, notA2)

    val nand = new Wire

    andGate(notA1, notA2, nand)

    inverter(nand, output)
  }

  def demux(in: Wire, c: List[Wire], out: List[Wire]) {
    if (c.isEmpty) {
      val signal: Wire = new Wire
      andGate(in, signal, out.head)
      signal.setSignal(true)
    } else {
      val inDemuxLow  = new Wire
      val inDemuxHigh = new Wire

      val highestWire: Wire = c.head
      val restOfControl = c.tail

      val invertedIn = new Wire

      val (outDemuxLow,outDemuxHigh) = out.splitAt(out.size/2)

      demux(inDemuxLow, restOfControl, outDemuxLow)
      demux(inDemuxHigh, restOfControl, outDemuxHigh)

      andGate(highestWire, in, inDemuxHigh)

      inverter(in, invertedIn)

      andGate(highestWire, in, inDemuxHigh)
      andGate(highestWire, invertedIn, inDemuxLow)


    }
  }

}

object Circuit extends CircuitSimulator {
  val InverterDelay = 1
  val AndGateDelay = 3
  val OrGateDelay = 5

  type TwoInputGate = (Wire, Wire, Wire) => Unit

  def twoInputGateExample(f: TwoInputGate) {
    val in1, in2, out = new Wire
    f(in1, in2, out)
    probe("in1", in1)
    probe("in2", in2)
    probe("out", out)
    in1.setSignal(false)
    in2.setSignal(false)
    run

    in1.setSignal(true)
    run

    in2.setSignal(true)
    run
  }

  def andGateExample() = twoInputGateExample(andGate)

  def orGateExample() = twoInputGateExample(orGate)

  def orGate2Example() = twoInputGateExample(orGate2)

  //
  // to complete with orGateExample and demuxExample...
  //
}

object CircuitMain extends App {
  // You can write tests either here, or better in the test class CircuitSuite.
  //  Circuit.andGateExample
  //  Circuit.orGateExample
  Circuit.orGate2Example()
}
