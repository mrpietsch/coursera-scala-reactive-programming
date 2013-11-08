package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll {
    a: Int =>
      val h = insert(a, empty)
      findMin(h) == a
  }

  property("min2") = forAll {
    (a: Int, b: Int) =>
      val h1 = insert(a, empty)
      val h2 = insert(b, h1)
      val theSmallerOne: Int = if (a < b) a else b
      findMin(h2) == theSmallerOne
  }

  property("oneInOneOut") = forAll {
    (a: Int, b: Int) =>
      val h1 = insert(a, empty)
      val h2 = deleteMin(h1)
      isEmpty(h2)
  }

  property("sameElementTwiceInserted") = forAll {
    a: Int =>
      val h1 = insert(a, empty)
      val h2 = insert(a, h1)
      val h3 = deleteMin(h2)
      val h4 = deleteMin(h3)
      isEmpty(h4)
  }

  property("whatGoesInMustComeOut") = forAll {
    s: Set[A] =>
      val allInserted = s.foldLeft(empty)((heap, el) => insert(el, heap))
      val allRemoved = exportHeapToSequence(allInserted)
      val allRemovedAsSet = allRemoved.toSet

      s == allRemovedAsSet
  }

  property("preserveAllElementsAndKeepMinimumOfOneWhileMerging") = forAll {
    (a: H, b: H) =>
      val m = meld(a, b)
      val sizeA = exportHeapToSequence(a).size
      val sizeB = exportHeapToSequence(b).size
      val sizeM = exportHeapToSequence(m).size

      val minA = findMin(a)
      val minB = findMin(b)
      val minM = findMin(m)

      (minM == minA || minM == minB) && (sizeM == sizeA + sizeB)
  }

  property("removeInSortedOrder") = forAll {
    heap: H => checkSorted(heap)
  }

  property("mergedShouldBeSorted") = forAll {
    (a: H, b: H) =>
      checkSorted(meld(a, b))
  }

  def exportHeapToSequence(h: H): Seq[A] = if (isEmpty(h)) Seq.empty else Seq(findMin(h)) ++ exportHeapToSequence(deleteMin(h))

  def checkSorted(heap: H) = {
    val elements = exportHeapToSequence(heap)
    val sortedA = elements.sorted

    elements == sortedA
  }

  lazy val genHeap: Gen[H] = for {
    i <- arbitrary[Int]
    h <- oneOf(value(empty), genHeap)
  } yield insert(i, h)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

}
