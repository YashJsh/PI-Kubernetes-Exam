import type { WorkflowStep, StepStatus } from "../types/workflow"

export function getReadySteps(
  steps: WorkflowStep[],
  stepStatus: Record<string, StepStatus>
): WorkflowStep[] {

  const sendingSteps = [];

  for (const step of steps) {
    const stepdependsOn = step.dependsOn;
    if (stepStatus[step.id] !== "PENDING") {
      continue;
    }

    if (!stepdependsOn || stepdependsOn.length === 0) {
      sendingSteps.push(step);
      continue;
    }


    let ready = true;
    for (const depend of stepdependsOn) {
      if (stepStatus[depend] !== "COMPLETED") {
        ready = false;
        break;
      }
    }

    if (ready) {
      sendingSteps.push(step);
    }
  }

  return sendingSteps;
}