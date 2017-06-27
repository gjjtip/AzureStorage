/**
 * Created by Administrator on 6/27/2017.
 */

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.core.Base64;
import com.microsoft.azure.storage.queue.*;

import java.util.EnumSet;

public class TestAzureStorageQueue {

    static String encodedKey = Base64.encode("Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".getBytes());
    public static final String storageConnectionString = "UseDevelopmentStorage=true";

    public void createQueue() {
        try {
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount =
                    CloudStorageAccount.parse(storageConnectionString);

            CloudStorageAccount.getDevelopmentStorageAccount();

            // Create the queue client.
            CloudQueueClient queueClient = storageAccount.createCloudQueueClient();

            // Retrieve a reference to a queue.
            CloudQueue queue = queueClient.getQueueReference("myqueue1");

            // Create the queue if it doesn't already exist.
            queue.createIfNotExists();
        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
        }
    }

    public void addMessageToQueue() {
        try {
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount =
                    CloudStorageAccount.parse(storageConnectionString);

            CloudStorageAccount.getDevelopmentStorageAccount();

            // Create the queue client.
            CloudQueueClient queueClient = storageAccount.createCloudQueueClient();

            // Retrieve a reference to a queue.
            CloudQueue queue = queueClient.getQueueReference("myqueue");

            // Create the queue if it doesn't already exist.
            queue.createIfNotExists();

            // Create a message and add it to the queue.
            for (int i = 0; i < 30; i++) {
                CloudQueueMessage message = new CloudQueueMessage("index:" + i);
                queue.addMessage(message);
            }

        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
        }
    }

    public void peekNextMessage() {
        try {
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount =
                    CloudStorageAccount.parse(storageConnectionString);

            CloudStorageAccount.getDevelopmentStorageAccount();

            // Create the queue client.
            CloudQueueClient queueClient = storageAccount.createCloudQueueClient();

            // Retrieve a reference to a queue.
            CloudQueue queue = queueClient.getQueueReference("myqueue");

            // Peek at the next message.
            CloudQueueMessage peekedMessage = queue.peekMessage();

            // Output the message value.
            if (peekedMessage != null) {
                System.out.println(peekedMessage.getMessageContentAsString());
            }
        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
        }
    }

    public void changeContentOfMessage() {
//        try {
//            // Retrieve storage account from connection-string.
//            CloudStorageAccount storageAccount =
//                    CloudStorageAccount.parse(storageConnectionString);
//            CloudStorageAccount.getDevelopmentStorageAccount();
//
//            // Create the queue client.
//            CloudQueueClient queueClient = storageAccount.createCloudQueueClient();
//
//            // Retrieve a reference to a queue.
//            CloudQueue queue = queueClient.getQueueReference("myqueue");
//
//            // The maximum number of messages that can be retrieved is 32.
//            final int MAX_NUMBER_OF_MESSAGES_TO_PEEK = 32;
//
//            // Loop through the messages in the queue.
//            for (CloudQueueMessage message : queue.retrieveMessages(MAX_NUMBER_OF_MESSAGES_TO_PEEK, 1, null, null)) {
//                // Check for a specific string.
//                if (message.getMessageContentAsString().equals("Hello, World")) {
//                    // Modify the content of the first matching message.
//                    message.setMessageContent("Updated contents.");
//                    // Set it to be visible in 30 seconds.
//                    EnumSet<MessageUpdateFields> updateFields =
//                            EnumSet.of(MessageUpdateFields.CONTENT,
//                                    MessageUpdateFields.VISIBILITY);
//                    // Update the message.
//                    queue.updateMessage(message, 30, updateFields, null, null);
//                    break;
//                }
//            }
//        } catch (Exception e) {
//            // Output the stack trace.
//            e.printStackTrace();
//        }
//    }

        try {
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount =
                    CloudStorageAccount.parse(storageConnectionString);

            CloudStorageAccount.getDevelopmentStorageAccount();

            // Create the queue client.
            CloudQueueClient queueClient = storageAccount.createCloudQueueClient();

            // Retrieve a reference to a queue.
            CloudQueue queue = queueClient.getQueueReference("myqueue");

            // Retrieve the first visible message in the queue.
            CloudQueueMessage message = queue.retrieveMessage();

            if (message != null) {
                // Modify the message content.
                message.setMessageContent("upupup");
                // Set it to be visible in 60 seconds.
                EnumSet<MessageUpdateFields> updateFields =
                        EnumSet.of(MessageUpdateFields.CONTENT,
                                MessageUpdateFields.VISIBILITY);
                // Update the message.
                queue.updateMessage(message, 60, updateFields, null, null);
            }
        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
        }
    }

    public void getQueueLength() {
        try {
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount =
                    CloudStorageAccount.parse(storageConnectionString);

            CloudStorageAccount.getDevelopmentStorageAccount();

            // Create the queue client.
            CloudQueueClient queueClient = storageAccount.createCloudQueueClient();

            // Retrieve a reference to a queue.
            CloudQueue queue = queueClient.getQueueReference("myqueue");

            // Download the approximate message count from the server.
            queue.downloadAttributes();

            // Retrieve the newly cached approximate message count.
            long cachedMessageCount = queue.getApproximateMessageCount();

            // Display the queue length.
            System.out.println(String.format("Queue length: %d", cachedMessageCount));
        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
        }
    }

    public void unqueueMessage() {
        try {
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount =
                    CloudStorageAccount.parse(storageConnectionString);

            CloudStorageAccount.getDevelopmentStorageAccount();

            // Create the queue client.
            CloudQueueClient queueClient = storageAccount.createCloudQueueClient();

            // Retrieve a reference to a queue.
            CloudQueue queue = queueClient.getQueueReference("myqueue");

            // Retrieve the first visible message in the queue.
//            CloudQueueMessage retrievedMessage = queue.retrieveMessage();

            final int MAX_NUMBER_OF_MESSAGES_TO_PEEK = 32;
            for (CloudQueueMessage message : queue.retrieveMessages(MAX_NUMBER_OF_MESSAGES_TO_PEEK, 1, null, null)) {
                // Check for a specific string.
                if (message.getMessageContentAsString().equals("go next!")) {
                    // Modify the content of the first matching message.
                    queue.deleteMessage(message);
                    break;
                }
            }
        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
        }
    }

    public void customizeMessage() {
        try {
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount =
                    CloudStorageAccount.parse(storageConnectionString);

            CloudStorageAccount.getDevelopmentStorageAccount();
            // Create the queue client.
            CloudQueueClient queueClient = storageAccount.createCloudQueueClient();

            // Retrieve a reference to a queue.
            CloudQueue queue = queueClient.getQueueReference("myqueue");

            // Retrieve 20 messages from the queue with a visibility timeout of 300 seconds.
            for (CloudQueueMessage message : queue.retrieveMessages(30, 60, null, null)) {
                // Do processing for all messages in less than 5 minutes,
                // deleting each message after processing.
                if (message.getMessageContentAsString().equals("index:10") || message.getMessageContentAsString().equals("index:0"))
                    queue.deleteMessage(message);
            }
        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
        }
    }

    public void listQueue() {
        try {
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount =
                    CloudStorageAccount.parse(storageConnectionString);

            CloudStorageAccount.getDevelopmentStorageAccount();

            // Create the queue client.
            CloudQueueClient queueClient =
                    storageAccount.createCloudQueueClient();

            // Loop through the collection of queues.
            for (CloudQueue queue : queueClient.listQueues()) {
                // Output each queue name.
                System.out.println(queue.getName());
            }
        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
        }
    }

    public void deleteQueue() {
        try {
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount =
                    CloudStorageAccount.parse(storageConnectionString);

            CloudStorageAccount.getDevelopmentStorageAccount();

            // Create the queue client.
            CloudQueueClient queueClient = storageAccount.createCloudQueueClient();

            // Retrieve a reference to a queue.
            CloudQueue queue = queueClient.getQueueReference("myqueue1");

            // Delete the queue if it exists.
            queue.deleteIfExists();
        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
        }
    }

}
