package example.swf.helloswf;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowClient;
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowClientBuilder;
import com.amazonaws.services.simpleworkflow.model.ActivityType;
import com.amazonaws.services.simpleworkflow.model.CompleteWorkflowExecutionDecisionAttributes;
import com.amazonaws.services.simpleworkflow.model.Decision;
import com.amazonaws.services.simpleworkflow.model.DecisionTask;
import com.amazonaws.services.simpleworkflow.model.DecisionType;
import com.amazonaws.services.simpleworkflow.model.HistoryEvent;
import com.amazonaws.services.simpleworkflow.model.PollForDecisionTaskRequest;
import com.amazonaws.services.simpleworkflow.model.RespondDecisionTaskCompletedRequest;
import com.amazonaws.services.simpleworkflow.model.ScheduleActivityTaskDecisionAttributes;
import com.amazonaws.services.simpleworkflow.model.TaskList;

/**
 * @author santosh.kothapalli
 *
 */
public class WorkflowWorker {
	public static void main(String[] args) {
		
		AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (C:\\Users\\santosh.kothapalli\\.aws\\credentials), and is in valid format.",
                    e);
        }
        AmazonSimpleWorkflowClient swf = (AmazonSimpleWorkflowClient)AmazonSimpleWorkflowClientBuilder.standard().withRegion(Regions.US_WEST_2).
        		withCredentials(new AWSStaticCredentialsProvider(credentials)).build();
        
	    PollForDecisionTaskRequest task_request =
	        new PollForDecisionTaskRequest()
	            .withDomain(HelloTypes.DOMAIN)
	            .withTaskList(new TaskList().withName(HelloTypes.TASKLIST));

	    while (true) {
	        System.out.println(
	                "Polling for a decision task from the tasklist '" +
	                HelloTypes.TASKLIST + "' in the domain '" +
	                HelloTypes.DOMAIN + "'.");

	        DecisionTask task = swf.pollForDecisionTask(task_request);

	        String taskToken = task.getTaskToken();
	        if (taskToken != null) {
	            try {
	                executeDecisionTask(swf, taskToken, task.getEvents());
	            } catch (Throwable th) {
	                th.printStackTrace();
	            }
	        }
	    }
	}
	
	private static void executeDecisionTask(AmazonSimpleWorkflowClient swf, String taskToken, List<HistoryEvent> events)
	        throws Throwable {
	    List<Decision> decisions = new ArrayList<Decision>();
	    String workflow_input = null;
	    int scheduled_activities = 0;
	    int open_activities = 0;
	    boolean activity_completed = false;
	    String result = null;
	    
	    System.out.println("Executing the decision task for the history events: [");
	    for (HistoryEvent event : events) {
	        System.out.println("  " + event);
	        switch(event.getEventType()) {
	            case "WorkflowExecutionStarted":
	                workflow_input =
	                    event.getWorkflowExecutionStartedEventAttributes()
	                         .getInput();
	                System.out.println("Workflow Input :  "+ workflow_input);
	                break;
	            case "ActivityTaskScheduled":
	                scheduled_activities++;
	                break;
	            case "ScheduleActivityTaskFailed":
	                scheduled_activities--;
	                break;
	            case "ActivityTaskStarted":
	                scheduled_activities--;
	                open_activities++;
	                break;
	            case "ActivityTaskCompleted":
	                open_activities--;
	                activity_completed = true;
	                result = event.getActivityTaskCompletedEventAttributes()
	                              .getResult();
	                break;
	            case "ActivityTaskFailed":
	                open_activities--;
	                break;
	            case "ActivityTaskTimedOut":
	                open_activities--;
	                break;
	        }
	    }
	    System.out.println("]");
	    
	    if (activity_completed) {
	        decisions.add(
	            new Decision()
	                .withDecisionType(DecisionType.CompleteWorkflowExecution)
	                .withCompleteWorkflowExecutionDecisionAttributes(
	                    new CompleteWorkflowExecutionDecisionAttributes()
	                        .withResult(result)));
	    } else {
	        if (open_activities == 0 && scheduled_activities == 0) {

	            ScheduleActivityTaskDecisionAttributes attrs =
	                new ScheduleActivityTaskDecisionAttributes()
	                    .withActivityType(new ActivityType()
	                        .withName(HelloTypes.ACTIVITY)
	                        .withVersion(HelloTypes.ACTIVITY_VERSION))
	                    .withActivityId(UUID.randomUUID().toString())
	                    .withInput(workflow_input);

	            decisions.add(
	                    new Decision()
	                        .withDecisionType(DecisionType.ScheduleActivityTask)
	                        .withScheduleActivityTaskDecisionAttributes(attrs));
	        } else {
	            // an instance of HelloActivity is already scheduled or running. Do nothing, another
	            // task will be scheduled once the activity completes, fails or times out
	        }
	    }

	    System.out.println("Exiting the decision task with the decisions " + decisions);
	    swf.respondDecisionTaskCompleted(
	    	    new RespondDecisionTaskCompletedRequest()
	    	        .withTaskToken(taskToken)
	    	        .withDecisions(decisions));
	}



}
