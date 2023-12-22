package io.orkes.demo.banking;

import com.netflix.conductor.common.metadata.tasks.*;
import com.netflix.conductor.common.metadata.workflow.*;
import com.netflix.conductor.common.run.*;
import com.netflix.conductor.sdk.workflow.executor.*;
import io.orkes.conductor.client.*;

import java.io.*;
import java.util.*;

public class Main {

    static String url = "CHANGE_ME";
    static String key = "CHANGE_ME";
    static String secret = "CHANGE_ME";

    static String WORKFLOW_NAME = "DemoWorkFlow";

    private Scanner scanner;

    private WorkflowClient workflowClient;

    TaskClient taskClient;

    public Main(Scanner scanner) {
        this.scanner = scanner;
        OrkesClients orkesClients = getOrkesClients();
        this.workflowClient = orkesClients.getWorkflowClient();
        this.taskClient = orkesClients.getTaskClient();

        WorkflowExecutor executor = new WorkflowExecutor(taskClient, orkesClients.getWorkflowClient(), orkesClients.getMetadataClient(), 100);

        //Important:  The following line scans all the classes for the worker classes with @WorkerTask annotations
        //Use it when implementing workers with annotations
        executor.initWorkers("io.orkes");


        System.out.printf("Started workers\n");
    }

    public OrkesClients getOrkesClients() {
        ApiClient apiClient = new ApiClient(url, key, secret);
        OrkesClients orkesClients = new OrkesClients(apiClient);
        return orkesClients;
    }



    public String startWorkflow() {

        StartWorkflowRequest request = new StartWorkflowRequest();
        request.setName(WORKFLOW_NAME);
        request.setInput(Map.of("i_currency", "USD", "o_currency", "AED"));

        String workflowId = workflowClient.startWorkflow(request);
        return workflowId;
    }

    private Workflow getWorkflow(String workflowId) {
        System.out.println("Getting " + workflowId);
        return workflowClient.getWorkflow(workflowId, true);
    }
    private void terminate(String workflowId) {
        workflowClient.terminateWorkflow(workflowId, "terminated by user");
    }

    private void completeTask(Task pending) {
        taskClient.updateTaskSync(pending.getWorkflowInstanceId(), pending.getReferenceTaskName(), TaskResult.Status.COMPLETED,
                Map.of("key1", "value1"));
    }

    private void resumeSession(String workflowId) {
        workflowClient.retryLastFailedTask(workflowId);
    }


    public static void main(String[] args) throws IOException {
        Scanner scanner = new Scanner(System.in);
        scanner.useDelimiter("\\s*");

        Main main = new Main(scanner);
        String workflowId = null;
        Task pending = null;
        while (true) {
            int option = main.readOptions();
            switch (option) {
                case 1:
                    workflowId = main.startWorkflow();
                    System.out.println("Started session: " + workflowId);
                    break;
                case 2:
                    System.out.println("Input the session Id (workflow Id): ");
                    scanner.skip("\n");
                    workflowId = scanner.nextLine();

                    Workflow workflow = main.getWorkflow(workflowId);
                    System.out.println("User session Status: " + workflow.getStatus());
                    Optional<Task> pendingOptinal = workflow.getTasks().stream().filter(t -> !t.getStatus().isTerminal()).findFirst();
                    if(pendingOptinal.isPresent()){
                        pending = pendingOptinal.get();
                    }
                    System.out.println("User session Current Pending Task: " + pending);
                    break;
                case 3:
                    if(workflowId == null) {
                        System.out.println("Input the session Id (workflow Id): ");
                        scanner.skip("\n");
                        workflowId = scanner.nextLine();
                    }
                    main.terminate(workflowId);
                    System.out.println("Terminated " + workflowId);
                    break;
                case 4:
                    if(workflowId == null) {
                        System.out.println("Input the session Id (workflow Id): ");
                        scanner.skip("\n");
                        workflowId = scanner.nextLine();
                    }
                    workflow = main.getWorkflow(workflowId);
                    if(workflow.getStatus().isTerminal()) {
                        //Let's retry the workflow
                        main.resumeSession(workflowId);
                        workflow = main.getWorkflow(workflowId);
                    }
                    pendingOptinal = workflow.getTasks().stream().filter(t -> !t.getStatus().isTerminal()).findFirst();
                    if(pendingOptinal.isPresent()){
                        pending = pendingOptinal.get();
                        main.completeTask(pending);
                    } else {
                        System.out.println("No pending tasks in the workflow.  User journey is complete");
                    }
                    break;
                case 5:
                    System.exit(1);
            }
        }
    }



    private int readOptions() throws IOException {
        System.out.println("Choose an option");
        System.out.println("[1] Start a new user session");
        System.out.println("[2] Retrieve a user session");
        System.out.println("[3] Terminate a user session");
        System.out.println("[4] Complete a task");
        System.out.println("[5] Exit");

        return scanner.nextInt();
    }
}
