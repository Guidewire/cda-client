package com.guidewire.cda.specs

import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.TableDrivenPropertyChecks

/**
  * A trait for Unit Testing. Any Unit Testing class needs to extend this trait. All necessary traits have been
  * extended as part of this trait.
  * Example of writing a Unit Test:
  *
  * <pre>
  * <code>
  * describe("A Set") {
  * describe("when empty") {
  * it("should have size 0") {
  * assert(Set.empty.size == 0)
  * }
  * *
  * it("should produce NoSuchElementException when head is invoked") {
  * ...
  * }
  * }
  * }
  * *
  *
  * describe("Sets") {
  * describe("when empty") {
  * val examples =
  * Table(
  * "set",
  *BitSet.empty,
  *HashSet.empty[Int],
  *TreeSet.empty[Int]
  * )
  * it("should produce NoSuchElementException when head is invoked") {
  * forAll(examples) { set =>
  *set.size should be(0)
  * }
  * }
  * }
  * }
  * </code>
  * </pre>
  *
  */
@RunWith(classOf[JUnitRunner])
abstract class UnitTestSpec extends FunSpec with TableDrivenPropertyChecks with Matchers with BeforeAndAfter with BeforeAndAfterAll with MockFactory {

  override def beforeAll(): Unit = {
    if (SparkSession.getActiveSession.isDefined) {
      SparkSession.getActiveSession.get.close
    }
    SparkSession.clearActiveSession
    SparkSession.clearDefaultSession
  }
}
