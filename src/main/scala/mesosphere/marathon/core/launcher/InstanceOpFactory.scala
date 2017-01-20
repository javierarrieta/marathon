package mesosphere.marathon
package core.launcher

import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.state.RunSpec
import mesosphere.util.state.FrameworkId
import org.apache.mesos.{ Protos => Mesos }

/** Infers which TaskOps to create for given run spec and offers. */
trait InstanceOpFactory {

  /**
    * Match an offer request.
    *
    * @param request the offer request.
    * @return Either this request results in a Match with some InstanceOp or a NoMatch
    *         which describes why this offer request could not be matched.
    */
  def matchOfferRequest(request: InstanceOpFactory.Request): OfferMatchResult

}

object InstanceOpFactory {
  /**
    * @param runSpec the related run specification definition
    * @param offer the offer to match against
    * @param instanceMap a map of all instances for the given run spec,
    *              needed to check constraints and handle resident tasks
    * @param additionalLaunches the number of additional launches that has been requested
    */
  // TODO: check whether instanceMap is always passed correctly
  case class Request(runSpec: RunSpec, offer: Mesos.Offer, instanceMap: Map[Instance.Id, Instance],
      additionalLaunches: Int) {
    def frameworkId: FrameworkId = FrameworkId("").mergeFromProto(offer.getFrameworkId)
    def isForResidentRunSpec: Boolean = runSpec.residency.isDefined

    lazy val instances: Stream[Instance] = instanceMap.values.to[Stream]

    /**
      * All available reservations, no matter what [[mesosphere.marathon.core.condition.Condition]] the instance is in.
      * This is needed so that Marathon can always match against an offer with a known reservation/persistent volume
      * and does not miss to match those because it thinks the instance is Unreachable.
      */
    lazy val availableReservations: Seq[Instance] = instances.filter(_.isReserved)

    /** The number of instances in state Reserved, meaning they're available to be launched */
    lazy val availableReservationsCount: Int = availableReservations.size
  }
}
