package example.swf.helloswf;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowClient;
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowClientBuilder;
import com.amazonaws.services.simpleworkflow.model.ActivityTask;
import com.amazonaws.services.simpleworkflow.model.PollForActivityTaskRequest;
import com.amazonaws.services.simpleworkflow.model.RespondActivityTaskCompletedRequest;
import com.amazonaws.services.simpleworkflow.model.RespondActivityTaskFailedRequest;
import com.amazonaws.services.simpleworkflow.model.TaskList;

/**
 * @author santosh.kothapalli
 * {http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/swf-hello.html}
 *
 */
public class ActivityWorker {
	
	private static String sayHello(String input) throws Throwable {
	    return "Hello, " + input + "!";
	}
	
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
        
	    while (true) {
	        System.out.println("Polling for an activity task from the tasklist '"
	                + HelloTypes.TASKLIST + "' in the domain '" +
	                HelloTypes.DOMAIN + "'.");

	        ActivityTask task = swf.pollForActivityTask(
	            new PollForActivityTaskRequest()
	                .withDomain(HelloTypes.DOMAIN)
	                .withTaskList(
	                    new TaskList().withName(HelloTypes.TASKLIST)));

	        String task_token = task.getTaskToken();
	        if (task_token != null) {
	            String result = null;
	            Throwable error = null;

	            try {
	                System.out.println("Executing the activity task with input '" +
	                        task.getInput() + "'.");
	                result = sayHello(task.getInput());
	            } catch (Throwable th) {
	                error = th;
	            }

	            if (error == null) {
	                System.out.println("The activity task succeeded with result '"
	                        + result + "'.");
	                swf.respondActivityTaskCompleted(
	                    new RespondActivityTaskCompletedRequest()
	                        .withTaskToken(task_token)
	                        .withResult(result));
	            } else {
	                System.out.println("The activity task failed with the error '"
	                        + error.getClass().getSimpleName() + "'.");
	                swf.respondActivityTaskFailed(
	                    new RespondActivityTaskFailedRequest()
	                        .withTaskToken(task_token)
	                        .withReason(error.getClass().getSimpleName())
	                        .withDetails(error.getMessage()));
	            }
	        }

	        
	    }
	    
	    

	}	
}
