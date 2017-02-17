package mesosphere.marathon
package core.storage.store

import java.io.File
import java.time.{ Clock, OffsetDateTime }
import java.util.zip.ZipFile

import akka.Done
import akka.http.scaladsl.marshalling.Marshaller
import akka.http.scaladsl.unmarshalling.Unmarshaller
import akka.stream.scaladsl.Sink
import mesosphere.AkkaUnitTest
import mesosphere.marathon.core.storage.backup.impl.ZipFileStorage
import mesosphere.marathon.core.storage.store.impl.BasePersistenceStore
import mesosphere.marathon.stream.Implicits._
import mesosphere.marathon.test.SettableClock

import scala.concurrent.Future
import scala.concurrent.duration._

case class TestClass1(str: String, int: Int, version: OffsetDateTime)

object TestClass1 {
  def apply(str: String, int: Int)(implicit clock: Clock): TestClass1 = {
    TestClass1(str, int, OffsetDateTime.now(clock))
  }
}

private[storage] trait PersistenceStoreTest { this: AkkaUnitTest =>
  def basicPersistenceStore[K, C, Serialized](name: String, newStore: => PersistenceStore[K, C, Serialized])(
    implicit
    ir: IdResolver[String, TestClass1, C, K],
    m: Marshaller[TestClass1, Serialized],
    um: Unmarshaller[Serialized, TestClass1]): Unit = {

    name should {
      "have no ids" in {
        val store = newStore
        store.ids().runWith(Sink.seq).futureValue should equal(Nil)
      }
      "have no keys" in {
        val store = newStore
        store match {
          case s: BasePersistenceStore[_, _, _] =>
            s.allKeys().runWith(Sink.seq).futureValue should equal(Nil)
          case _ =>
        }
      }
      "not fail if the key doesn't exist" in {
        val store = newStore
        store.get("task-1").futureValue should be('empty)
      }
      "create and list an object" in {
        implicit val clock = new SettableClock()
        val store = newStore
        val tc = TestClass1("abc", 1)
        store.store("task-1", tc).futureValue should be(Done)
        store.get("task-1").futureValue.value should equal(tc)
        store.ids().runWith(Sink.seq).futureValue should contain theSameElementsAs Seq("task-1")
        store.versions("task-1").runWith(Sink.seq).futureValue should contain theSameElementsAs Seq(tc.version)
      }
      "update an object" in {
        implicit val clock = new SettableClock()
        val store = newStore
        val original = TestClass1("abc", 1)
        clock.plus(1.minute)
        val updated = TestClass1("def", 2)
        store.store("task-1", original).futureValue should be(Done)
        store.store("task-1", updated).futureValue should be(Done)
        store.get("task-1").futureValue.value should equal(updated)
        store.get("task-1", original.version).futureValue.value should equal(original)
        store.versions("task-1").runWith(Sink.seq).futureValue should contain theSameElementsAs
          Seq(original.version, updated.version)
      }
      "delete idempontently" in {
        implicit val clock = new SettableClock()
        val store = newStore
        store.deleteAll("task-1").futureValue should be(Done)
        store.store("task-2", TestClass1("def", 2)).futureValue should be(Done)
        store.deleteAll("task-2").futureValue should be(Done)
        store.deleteAll("task-2").futureValue should be(Done)
      }
      "store the multiple versions of the old values" in {
        val clock = new SettableClock()
        val versions = 0.until(10).map { i =>
          clock.plus(1.minute)
          TestClass1("abc", i, OffsetDateTime.now(clock))
        }
        val store = newStore
        versions.foreach { v =>
          store.store("task", v).futureValue should be(Done)
        }
        clock.plus(1.hour)
        val newestVersion = TestClass1("def", 3, OffsetDateTime.now(clock))
        store.store("task", newestVersion).futureValue should be(Done)
        // it should have dropped one element.
        val storedVersions = store.versions("task").runWith(Sink.seq).futureValue
        // the current version is listed too.
        storedVersions should contain theSameElementsAs newestVersion.version +: versions.map(_.version)
        versions.foreach { v =>
          store.get("task", v.version).futureValue.value should equal(v)
        }
      }
      "allow storage of a value at a specific version even if the value doesn't exist in an unversioned slot" in {
        val store = newStore
        implicit val clock = new SettableClock()
        val tc = TestClass1("abc", 1)
        store.store("test", tc, tc.version).futureValue should be(Done)
        store.ids().runWith(Sink.seq).futureValue should contain theSameElementsAs Seq("test")
        store.get("test").futureValue should be('empty)
        store.get("test", tc.version).futureValue.value should be(tc)
        store.versions("test").runWith(Sink.seq).futureValue should contain theSameElementsAs Seq(tc.version)
        store.deleteVersion("test", tc.version).futureValue should be(Done)
        store.versions("test").runWith(Sink.seq).futureValue should be('empty)
      }
      "allow storage of a value at a specific version without replacing the existing one" in {
        val store = newStore
        implicit val clock = new SettableClock()
        val tc = TestClass1("abc", 1)
        val old = TestClass1("def", 2, OffsetDateTime.now(clock).minusHours(1))
        store.store("test", tc).futureValue should be(Done)
        store.store("test", old, old.version).futureValue should be(Done)
        store.versions("test").runWith(Sink.seq).futureValue should contain theSameElementsAs
          Seq(tc.version, old.version)
        store.get("test").futureValue.value should equal(tc)
        store.get("test", old.version).futureValue.value should equal(old)
        store.deleteAll("test").futureValue should be(Done)
        store.get("test").futureValue should be('empty)
      }
      "be able to backup and restore the state" in {
        Given("A persistent store with some data")
        val store = newStore
        implicit val clock = new SettableClock()
        val numEntries = 3
        val content = 0.until(numEntries).map(num => TestClass1(s"name-$num", num))
        Future.sequence(content.map(item => store.store(item.str, item))).futureValue
        val file = File.createTempFile("marathon-zipfile", ".zip")
        file.deleteOnExit()
        val zipSink = ZipFileStorage.sink(file)

        When("A backup is created")
        store.backup().runWith(zipSink).futureValue

        Then("The backup has all the content")
        val zipFile = new ZipFile(file)
        zipFile.entries().toSeq.size should be >= numEntries

        When("State is restored from the backup")
        val zipSource = ZipFileStorage.source(file)
        store.restore(zipSource).futureValue

        Then("The state is restored completely")
        val children = store.backup().runWith(stream.Sink.seq).futureValue
        children.size should be >= numEntries
        content.foreach { item =>
          store.get(item.str).futureValue should be(defined)
        }
        file.delete()
      }
    }
  }
}
