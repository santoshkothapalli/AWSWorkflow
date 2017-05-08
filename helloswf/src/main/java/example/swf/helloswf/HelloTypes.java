package example.swf.helloswf;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowClient;
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowClientBuilder;
import com.amazonaws.services.simpleworkflow.model.ChildPolicy;
import com.amazonaws.services.simpleworkflow.model.DomainAlreadyExistsException;
import com.amazonaws.services.simpleworkflow.model.RegisterActivityTypeRequest;
import com.amazonaws.services.simpleworkflow.model.RegisterDomainRequest;
import com.amazonaws.services.simpleworkflow.model.RegisterWorkflowTypeRequest;
import com.amazonaws.services.simpleworkflow.model.TaskList;
import com.amazonaws.services.simpleworkflow.model.TypeAlreadyExistsException;

/**
 * Hello world!
 * @author santosh.kothapalli
 *
 */
public class HelloTypes {
    public static final String DOMAIN = "HelloDomain";
    public static final String TASKLIST = "HelloTasklist";
    public static final String WORKFLOW = "HelloWorkflow";
    public static final String WORKFLOW_VERSION = "1.0";
    public static final String ACTIVITY = "HelloActivity";
    public static final String ACTIVITY_VERSION = "1.0";
    

    
    public static void registerDomain(AmazonSimpleWorkflowClient swf) {
        try {
            System.out.println("** Registering the domain '" + DOMAIN + "'.");
            swf.registerDomain(new RegisterDomainRequest()
                .withName(DOMAIN)
                .withWorkflowExecutionRetentionPeriodInDays("1"));
        } catch (DomainAlreadyExistsException e) {
            System.out.println("** Domain already exists!");
        }
    }
    
    public static void registerActivityType(AmazonSimpleWorkflowClient swf) {
        try {
            System.out.println("** Registering the activity type '" + ACTIVITY +
                "-" + ACTIVITY_VERSION + "'.");
            swf.registerActivityType(new RegisterActivityTypeRequest()
                .withDomain(DOMAIN)
                .withName(ACTIVITY)
                .withVersion(ACTIVITY_VERSION)
                .withDefaultTaskList(new TaskList().withName(TASKLIST))
                .withDefaultTaskScheduleToStartTimeout("30")
                .withDefaultTaskStartToCloseTimeout("600")
                .withDefaultTaskScheduleToCloseTimeout("630")
                .withDefaultTaskHeartbeatTimeout("10"));
        } catch (TypeAlreadyExistsException e) {
            System.out.println("** Activity type already exists!");
        }
    }
    
    public static void registerWorkflowType(AmazonSimpleWorkflowClient swf) {
        try {
            System.out.println("** Registering the workflow type '" + WORKFLOW +
                "-" + WORKFLOW_VERSION + "'.");
            swf.registerWorkflowType(new RegisterWorkflowTypeRequest()
                .withDomain(DOMAIN)
                .withName(WORKFLOW)
                .withVersion(WORKFLOW_VERSION)
                .withDefaultChildPolicy(ChildPolicy.TERMINATE)
                .withDefaultTaskList(new TaskList().withName(TASKLIST))
                .withDefaultTaskStartToCloseTimeout("30"));
        } catch (TypeAlreadyExistsException e) {
            System.out.println("** Workflow type already exists!");
        }
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
        registerDomain(swf);
        registerWorkflowType(swf);
        registerActivityType(swf);
    }
}