import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{Inside, OptionValues}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import ValveCtrlState.{
  Composite,
  ConsecutiveFailures,
  OverallQueuedAboveThreshold,
  SingleNodesAboveThreshold,
  ValveOpen
}
import scala.util.{Failure, Success}

class ValveCtrlStateTests extends AnyFunSpec with Matchers with Inside with OptionValues with LazyLogging {

  describe("ValveCtrlState updates") {
    describe("in a cluster of 10 nodes should") {
      val update = ValveCtrlState.updater(250, 500, 1000, 2500, 3)
      it("keep valve open if all is fine") {
        val nodesStatesLastSample = Vector.tabulate(10) { i => SearchQueueState(s"datanode_$i", 1000, 100) }
        val nextState = update(ValveCtrlState.valveOpen, Success(nodesStatesLastSample))
        nextState shouldBe ValveOpen
      }
      it("close valve if 1 node is above threshold") {
        val nodesStatesLastSample = Vector.tabulate(9) { i =>
          SearchQueueState(s"datanode_$i", 1000, 10)
        } :+ SearchQueueState("datanode_10", 1000, 501)
        val nextState = update(ValveCtrlState.valveOpen, Success(nodesStatesLastSample))
        nextState shouldEqual SingleNodesAboveThreshold(Set("datanode_10"))
      }
      it("close valve if overall cluster is above threshold") {
        val nodesStatesLastSample = Vector.tabulate(10) { i => SearchQueueState(s"datanode_$i", 1000, 251) }
        val nextState = update(ValveCtrlState.valveOpen, Success(nodesStatesLastSample))
        nextState shouldBe OverallQueuedAboveThreshold
      }
      it("close valve if 1 node is above threshold AND overall cluster is above threshold") {
        val nodesStatesLastSample = Vector.tabulate(9) { i =>
          SearchQueueState(s"datanode_$i", 1000, 250)
        } :+ SearchQueueState("datanode_10", 1000, 501)
        val nextState = update(ValveCtrlState.valveOpen, Success(nodesStatesLastSample))
        nextState shouldEqual Composite(
          List(
            OverallQueuedAboveThreshold,
            SingleNodesAboveThreshold(Set("datanode_10"))
          )
        )
      }
      it("close valve if _cat/thread_pool API call failed") {
        val nextState = update(ValveCtrlState.valveOpen, Failure(new Exception))
        nextState shouldBe ConsecutiveFailures(1)
      }
      it("accumulate _cat/thread_pool failures") {
        val nextState = update(ValveCtrlState.consecutiveFailures(1), Failure(new Exception))
        nextState shouldBe ConsecutiveFailures(2)
      }
      it("accumulate _cat/thread_pool failures up to the threshold") {
        val nextState = update(ValveCtrlState.consecutiveFailures(2), Failure(new Exception))
        nextState shouldBe ConsecutiveFailures(3)
      }
      it("not accumulate _cat/thread_pool failures above the threshold") {
        val nextState = update(ValveCtrlState.consecutiveFailures(3), Failure(new Exception))
        nextState shouldBe ConsecutiveFailures(3)
      }
      describe("decrement _cat/thread_pool fail count when call succeeds") {
        it("without any other issues") {
          val nodesStatesLastSample = Vector.tabulate(10) { i => SearchQueueState(s"datanode_$i", 1000, 100) }
          val nextState = update(ValveCtrlState.consecutiveFailures(3), Success(nodesStatesLastSample))
          nextState shouldBe ConsecutiveFailures(2)
        }
        it("but also block if 1 node is above threshold") {
          val nodesStatesLastSample = Vector.tabulate(9) { i =>
            SearchQueueState(s"datanode_$i", 1000, 10)
          } :+ SearchQueueState("datanode_10", 1000, 501)
          val nextState = update(ValveCtrlState.consecutiveFailures(3), Success(nodesStatesLastSample))
          inside(nextState) {
            case Composite(blockers) => blockers should contain theSameElementsAs List(
              ConsecutiveFailures(2),
              SingleNodesAboveThreshold(Set("datanode_10"))
            )
          }
        }
        it("but also block if cluster is above threshold") {
          val nodesStatesLastSample = Vector.tabulate(10) { i => SearchQueueState(s"datanode_$i", 1000, 251) }
          val nextState = update(ValveCtrlState.consecutiveFailures(3), Success(nodesStatesLastSample))
          inside(nextState) {
            case Composite(blockers) => blockers should contain theSameElementsAs List(
              ConsecutiveFailures(2),
              OverallQueuedAboveThreshold
            )
          }
        }
      }
      describe("handle composite current state when") {
        val currentState = Composite(List(
          ValveCtrlState.consecutiveFailures(3),
          OverallQueuedAboveThreshold,
          SingleNodesAboveThreshold(Set("datanode_9"))
        ))
        it("everything is fine, but not under low threshold yet") {
          val nodesStatesLastSample = Vector.tabulate(9) { i =>
            SearchQueueState(s"datanode_$i", 1000, 100)
          } :+ SearchQueueState("datanode_9", 1000, 251)
          val nextState = update(currentState, Success(nodesStatesLastSample))
          inside(nextState) {
            case Composite(blockers) => blockers should contain theSameElementsAs List(
              ConsecutiveFailures(2),
              OverallQueuedAboveThreshold,
              SingleNodesAboveThreshold(Set("datanode_9"))
            )
          }
        }
        it("everything is fine, cluster above low, and some (not same) node is above low but under high threshold") {
          val nodesStatesLastSample = Vector.tabulate(9) { i =>
            SearchQueueState(s"datanode_${i + 1}", 1000, 100)
          } :+ SearchQueueState("datanode_0", 1000, 251)
          val nextState = update(currentState, Success(nodesStatesLastSample))
          inside(nextState) {
            case Composite(blockers) => blockers should contain theSameElementsAs List(
              ConsecutiveFailures(2),
              OverallQueuedAboveThreshold
            )
          }
        }
        it("everything is fine, cluster above low, and some (not same) node is above high threshold") {
          val nodesStatesLastSample = Vector.tabulate(9) { i =>
            SearchQueueState(s"datanode_${i + 1}", 1000, 100)
          } :+ SearchQueueState("datanode_0", 1000, 501)
          val nextState = update(currentState, Success(nodesStatesLastSample))
          inside(nextState) {
            case Composite(blockers) => blockers should contain theSameElementsAs List(
              ConsecutiveFailures(2),
              OverallQueuedAboveThreshold,
              SingleNodesAboveThreshold(Set("datanode_0"))
            )
          }
        }
      }
    }
  }
}
