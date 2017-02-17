package mesosphere.marathon
package stream

import akka.util.ByteString
import java.io.ByteArrayInputStream
import mesosphere.{ AkkaTest, UnitTest }

import akka.stream.scaladsl.Source
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import scala.annotation.tailrec

class TarFlowTest extends UnitTest with AkkaTest {
  import TarFlow._

  val tarEntries = List(
    TarEntry(
      "1234567890/1234567890/1234567890/1234567890/1234567890/1234567890/1234567890/1234567890/1234567890/1234567890/long.txt",
      ByteString("> 100 char file name. Look out!")),
    TarEntry(
      "/path/to/file-2.txt",
      ByteString("Very file! Such data 2")),
    TarEntry(
      "/path/to/file.txt",
      ByteString("Very file! Such data")))

  val tarredBytes =
    Source(tarEntries).
      via(TarFlow.writer).
      runReduce(_ ++ _).
      futureValue

  List(1, 13, 512, Int.MaxValue).foreach { n =>
    s"it can roundtrip tar and untar with ${n} sized byte boundaries" in {
      val untarredItems =
        Source(tarredBytes.grouped(n).toList). // we vary the chunk sizes to make sure we handle boundaries properly
          via(TarFlow.reader).
          runFold(List.empty[TarEntry]) { _ :+ _ }.
          futureValue
      untarredItems.map(_.header.getName) shouldBe tarEntries.map(_.header.getName)
      untarredItems.map(_.data) shouldBe tarEntries.map(_.data)
    }
  }

  "it generates valid tar data that can be properly read by apache commons tar" in {
    val bytes = new ByteArrayInputStream(tarredBytes.toArray)
    val tar = new TarArchiveInputStream(bytes)

    var entries = List.empty[TarEntry]
    @tailrec def readEntries(tar: TarArchiveInputStream, entries: List[TarEntry] = Nil): List[TarEntry] = {
      val entry = tar.getNextTarEntry
      if (entry == null)
        entries
      else {
        val data = Array.ofDim[Byte](entry.getSize.toInt)
        tar.read(data)
        readEntries(tar, entries :+ TarEntry(entry, ByteString(data)))
      }
    }

    val untarredItems = readEntries(tar)
    untarredItems.map(_.header.getName) shouldBe tarEntries.map(_.header.getName)
    untarredItems.map(_.data) shouldBe tarEntries.map(_.data)
  }

}
