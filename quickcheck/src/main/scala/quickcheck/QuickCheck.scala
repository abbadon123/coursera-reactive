package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  lazy val genHeap: Gen[H] = for {
    x <- arbitrary[Int]
    h <- oneOf(const(empty), genHeap)
  } yield insert(x, h)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

  property("gen1") = forAll { (h: H) =>
    val m = if (isEmpty(h)) 0 else findMin(h)
    findMin(insert(m, h)) == m
  }

  property("""
    If you insert any two elements into an empty heap, finding the 
    minimum of the resulting heap should get the smallest of the 
    two elements back.
    """) = forAll { (x1: Int, x2: Int) =>
    val h = insert(x1, insert(x2, empty))
    findMin(h) == Math.min(x1, x2)
  }
  
  property("""
      If you insert an element into an empty heap, then delete the minimum, the resulting heap should be empty.
    """) = forAll { x1: Int =>
    val h = insert(x1, empty)
    deleteMin(h) == empty
  }
    
  property("""
      Given any heap, you should get a sorted sequence of elements when continually 
      finding and deleting minima. 
      (Hint: recursion and helper functions are your friends.)
    """) = forAll { h: H =>
  
    def asList(h : H) : List[Int] = {
      if (isEmpty(h)) List()
      else findMin(h) :: asList(deleteMin(h))
    }    
      
    val xs = asList(h)
    xs == xs.sorted
  }
  
  property("""
      Finding a minimum of the melding of any two heaps should return a minimum of one or the other.
    """) = forAll { (h1: H, h2: H) =>
    findMin( meld(h1, h2) ) == Math.min(findMin(h1), findMin(h2))
  }
  
  // TODO find them all !
  

}
