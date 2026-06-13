import type { StepResult, StepState, StepStatus, Workflow } from "../types/workflow"
import { getWorkflow, setWorkflow } from "./workflow-store"
import { getReadySteps } from "./dag"
import { stepQueue } from "../queue/step-queue"
import { resultQueue } from "../queue/result-queue"
import { podManager } from "../pod-manager/pod-manager"

/**
 * TODO: Implement this class.
 *
 * The orchestrator is the brain of the system. It ties together
 * the DAG resolver, step queue, result queue, and pod manager.
 *
 * submitWorkflow(workflow):
 *   1. Store workflow in workflow-store with all steps as PENDING
 *   2. Run getReadySteps() to find immediately runnable steps
 *   3. Push each ready step into the step queue
 *   4. Mark those steps as QUEUED in workflow-store
 *
 * IMPORTANT: Store the workflow state before enqueueing the first ready step.
 * Result events can come back quickly, and the result handler must be able
 * to find the workflow in workflow-store.
 *
 * start():
 *   - Begin consuming from the result queue
 *   - Begin draining the step queue (send steps to pod manager)
 *   - For Section 1: sequential is fine
 *   - For Section 2: run parallel dispatch
 *
 * On StepResult received (from result queue consumer):
 *   1. Update the step's status in workflow-store
 *   2. If COMPLETED: run getReadySteps() again, enqueue newly unblocked steps
 *   3. Check if all steps done → update workflow status to "completed" or "failed"
 *
 * INVARIANT: Backend is the ONLY place that writes stepStatus.
 * Pod manager only pushes events. Orchestrator reads events and updates state.
 */
export class Orchestrator {
  async submitWorkflow(workflow: Workflow): Promise<void> {

    const stepState: Record<string, StepState> = {};
    const stepStatus: Record<string, StepStatus> = {};

    for (const step of workflow.steps) {
      stepState[step.id] = {
        podId: null,
        status: "PENDING",
        stepId: step.id
      };

      stepStatus[step.id] = "PENDING";
    }

    setWorkflow(workflow.workflowId, {
      workflowId: workflow.workflowId,
      status: "pending",
      steps: workflow.steps,
      stepState: stepState
    });

    const readySteps = getReadySteps(workflow.steps, stepStatus);

    const storedWorkflow = getWorkflow(workflow.workflowId);
    if (!storedWorkflow) {
      throw new Error("Workflow is not present");
    }

    for (const step of readySteps) {
      storedWorkflow.stepState[step.id] = {
        status: "QUEUED",
        podId: null,
        stepId: step.id
      };
    }

    for (const step of readySteps) {
      console.log("Enqueing step : ", step);
      stepQueue.enqueue({
        command: step.command,
        enqueuedAt: Date.now(),
        stepId: step.id,
        workflowId: workflow.workflowId
      });
    }

    setWorkflow(workflow.workflowId, storedWorkflow);
  }

  async start(): Promise<void> {
    void resultQueue;
    void podManager;

    const result = async (result: StepResult) => {
      const workflow = getWorkflow(result.workflowId);
      if (!workflow) {
        throw new Error("Workflow not found");
      }

      const step = workflow.stepState[result.stepId];
      if (step) {
        step.status = result.status;
        step.podId = result.podId;
        step.stdout = result.stdout ?? step.stdout;
        step.exitCode = result.exitCode ?? step.exitCode;
        step.error = result.error ?? step.error;
      }

      const stepStatus: Record<string, StepStatus> = {};
      for (const s of workflow.steps) {
        stepStatus[s.id] = workflow.stepState[s.id].status;
      }

      if (result.status === "COMPLETED") {
        const readySteps = getReadySteps(workflow.steps, stepStatus);

        for (const readyStep of readySteps) {
          workflow.stepState[readyStep.id] = {
            status: "QUEUED",
            podId: null,
            stepId: readyStep.id
          };

          stepQueue.enqueue({
            stepId: readyStep.id,
            workflowId: workflow.workflowId,
            command: readyStep.command,
            enqueuedAt: Date.now()
          });
        }
      }

      let allDone = true;
      let anyFailed = false;

      for (const s of workflow.steps) {
        const status = workflow.stepState[s.id].status;

        if (status === "FAILED") {
          anyFailed = true;
        }

        if (status !== "COMPLETED" && status !== "SKIPPED") {
          allDone = false;
        }
      }

      if (allDone) {
        workflow.status = anyFailed ? "failed" : "completed";
      }

      setWorkflow(workflow.workflowId, workflow);
    };

    resultQueue.consume(result);

    while (true) {
      const step = await stepQueue.dequeue();
      if (step) {
        console.log("Step arrived");
        podManager.dispatch(step).catch(console.error);
      } else {
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    }
  }
}

export const orchestrator = new Orchestrator()
