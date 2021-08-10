import scala.util.Try

sealed trait ValveCtrlState
object ValveCtrlState {

  case object ValveOpen extends ValveCtrlState
  case class SingleNodesAboveThreshold(nodes: Set[String]) extends ValveCtrlState
  case object OverallQueuedAboveThreshold extends ValveCtrlState
  case class ConsecutiveFailures(count: Int) extends ValveCtrlState
  case class Composite(aggregated: List[ValveCtrlState]) extends ValveCtrlState

  def valveOpen:                                     ValveCtrlState = ValveOpen
  def singleNodesAboveThreshold(nodes: Set[String]): ValveCtrlState = SingleNodesAboveThreshold(nodes)
  def overallQueuedAboveThreshold:                   ValveCtrlState = OverallQueuedAboveThreshold
  def consecutiveFailures(count: Int):               ValveCtrlState = ConsecutiveFailures(count)
  def composite(aggregated: List[ValveCtrlState]):   ValveCtrlState = Composite(aggregated)

  def unapply(tuple: (ValveCtrlState, ValveCtrlState)): Option[ValveCtrlState] = tuple match {
    case (ValveOpen, somethingElse)                                     => Some(somethingElse)
    case (somethingElse, ValveOpen)                                     => Some(somethingElse)
    case (SingleNodesAboveThreshold(n0), SingleNodesAboveThreshold(n1)) => Some(SingleNodesAboveThreshold(n0 union n1))
    case (OverallQueuedAboveThreshold, OverallQueuedAboveThreshold)     => Some(OverallQueuedAboveThreshold)
    case (ConsecutiveFailures(c0), ConsecutiveFailures(c1))             => Some(ConsecutiveFailures(math.min(c0, c1)))
    case _                                                              => None
  }

  def merge(lhs: ValveCtrlState, rhs: ValveCtrlState): ValveCtrlState = (lhs, rhs) match {
    case ValveCtrlState(sameNonCompositeMerged) => sameNonCompositeMerged
    case (Composite(l), c @ Composite(_)) => l.foldLeft[ValveCtrlState](c)(merge)
    case (l, r: Composite) => merge(r, l)
    case (Composite(l), r) => Composite(l.foldLeft(r :: Nil) { case (head :: tail, vcs) => (head, vcs) match {
        case ValveCtrlState(sameNonCompositeMerged) => sameNonCompositeMerged :: tail
        case _ => head :: vcs :: tail
      }
    })
    case (nonComp0, nonComp1) => Composite(nonComp0 :: nonComp1 :: Nil)
  }

  type NStates = Try[Vector[SearchQueueState]]

  def updater(lowNodeThreshold:     Int,
              highNodeThreshold:    Int,
              lowClusterThreshold:  Int,
              highClusterThreshold: Int,
              maxFailuresCount:     Int): (ValveCtrlState, NStates) => ValveCtrlState = {

    val f1 = onValveOpen(highNodeThreshold, highClusterThreshold)
    val f2 = onSingleNodesAboveThreshold(lowNodeThreshold, highNodeThreshold, highClusterThreshold)
    val f3 = onOverallQueuedAboveThreshold(highNodeThreshold, lowClusterThreshold)
    val f4 = onConsecutiveFailures(maxFailuresCount).compose(f1)

    (valveCtrlState, states) => valveCtrlState match {
      case ValveOpen => f1(states)
      case SingleNodesAboveThreshold(nodes) => f2(states, nodes)
      case OverallQueuedAboveThreshold => f3(states)
      case ConsecutiveFailures(count) => f4(states)(count)
      case Composite(list) => list.map {
        case SingleNodesAboveThreshold(nodes) => f2(states, nodes)
        case OverallQueuedAboveThreshold => f3(states)
        case ConsecutiveFailures(count) => f4(states)(count)
      }.reduce(merge)
    }
  }

  val computeState: (Set[String], Boolean) => ValveCtrlState = (overflowedNodes, clusterOverflowed) =>
    if (overflowedNodes.isEmpty) {
      if (clusterOverflowed) OverallQueuedAboveThreshold
      else ValveOpen
    } else {
      val partial = SingleNodesAboveThreshold(overflowedNodes)
      if (clusterOverflowed) Composite(OverallQueuedAboveThreshold :: partial :: Nil)
      else partial
    }

  def computeStateFromTry(maybeStates: NStates)
                         (ifSuccess: Vector[SearchQueueState] => (Set[String], Boolean)): ValveCtrlState =
    maybeStates.fold(
      _ => ConsecutiveFailures(1),
      computeState.tupled.compose(ifSuccess))

  def onValveOpen(highNodeThreshold:    Int,
                  highClusterThreshold: Int): NStates => ValveCtrlState = { maybeStates =>
    computeStateFromTry(maybeStates) { states =>
      val overflowedNodes: Set[String] = states.view.collect {
        case s if s.tasksQueued > highNodeThreshold => s.nodeName
      }.toSet
      val clusterOverflowed = states.foldLeft(0)(_ + _.tasksQueued) > highClusterThreshold
      overflowedNodes -> clusterOverflowed
    }
  }

  def onSingleNodesAboveThreshold(lowNodeThreshold:     Int,
                                  highNodeThreshold:    Int,
                                  highClusterThreshold: Int): (NStates, Set[String]) => ValveCtrlState = { (ss, ns) =>
    computeStateFromTry(ss) { states =>
      val overflowedNodes: Set[String] = states.view.collect {
        case s if s.tasksQueued > highNodeThreshold => s.nodeName
      }.toSet

      val backToNormalNodes = states.view.collect { case s if s.tasksQueued < lowNodeThreshold => s.nodeName }.toSet
      val waitingToReturnToNormalNodes: Set[String] = overflowedNodes.union(ns.diff(backToNormalNodes))
      val clusterOverflowed = states.foldLeft(0)(_ + _.tasksQueued) > highClusterThreshold
      waitingToReturnToNormalNodes -> clusterOverflowed
    }
  }

  def onOverallQueuedAboveThreshold(highNodeThreshold:    Int,
                                    lowClusterThreshold:  Int): NStates => ValveCtrlState = { ss =>
    computeStateFromTry(ss) { states =>
      val overflowedNodes: Set[String] = states.view.collect {
        case s if s.tasksQueued > highNodeThreshold => s.nodeName
      }.toSet
      val clusterNotBackToNormal = states.foldLeft(0)(_ + _.tasksQueued) > lowClusterThreshold
      overflowedNodes -> clusterNotBackToNormal
    }
  }

  def onConsecutiveFailures(max: Int): ValveCtrlState => Int => ValveCtrlState = {
    case ConsecutiveFailures(c) => count => ConsecutiveFailures(math.min(count + c, max))
    case c @ ValveOpen          => count => if (count == 0) c else ConsecutiveFailures(count - 1)
    case c @ Composite(list)    => count => if (count == 0) c else Composite(ConsecutiveFailures(count - 1) :: list)
    case c                      => count => if (count == 0) c else Composite(ConsecutiveFailures(count - 1) :: c :: Nil)
  }
}
