package mesosphere.marathon
package state

import mesosphere.marathon.stream.Implicits._
import org.apache.mesos.{ Protos => mesos }

case class IpAddress(
    groups: Seq[String] = Seq.empty,
    labels: Map[String, String] = Map.empty[String, String],
    discoveryInfo: DiscoveryInfo = DiscoveryInfo.empty,
    networkName: Option[String] = None) {

  def toProto: Protos.IpAddress = {
    val builder = Protos.IpAddress.newBuilder
    groups.foreach(builder.addGroups)
    labels
      .map { case (key, value) => mesos.Label.newBuilder.setKey(key).setValue(value).build }
      .foreach(builder.addLabels)
    builder.setDiscoveryInfo(discoveryInfo.toProto)
    networkName.foreach { name => builder.setNetworkName(name) }
    builder.build
  }
}

object IpAddress {
  def empty: IpAddress = IpAddress()

  def fromProto(proto: Protos.IpAddress): IpAddress = {
    IpAddress(
      groups = proto.getGroupsList.toIndexedSeq,
      labels = proto.getLabelsList.map { p => p.getKey -> p.getValue }(collection.breakOut),
      discoveryInfo =
        if (proto.hasDiscoveryInfo) DiscoveryInfo.fromProto(proto.getDiscoveryInfo)
        else DiscoveryInfo.empty,
      networkName =
        if (proto.hasNetworkName) Some(proto.getNetworkName)
        else None
    )
  }
}
