// Include the following imports to use table APIs

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.core.Base64;
import com.microsoft.azure.storage.table.*;
import com.microsoft.azure.storage.table.TableQuery.*;

/**
 * Created by Administrator on 6/23/2017.
 */
public class TestAzureStorageTable {
    // Define the connection-string with your values.
    static String encodedKey = Base64.encode("Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".getBytes());
    public static final String storageConnectionString = "UseDevelopmentStorage=true";
//            "DefaultEndpointsProtocol=http;" +
//                    "AccountName=devstoreaccount1;" +
//                    "AccountKey=" + Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;
//    String storageConnectionString =
//            RoleEnvironment.getConfigurationSettings().get("StorageConnectionString");

    public void createNewTable() {
        try {
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount =
                    CloudStorageAccount.parse(storageConnectionString);

            CloudStorageAccount.getDevelopmentStorageAccount();

//            CloudStorageAccount.getDevelopmentStorageAccount();
            System.out.println(storageAccount.getBlobEndpoint());

            // Create the table client.
            CloudTableClient tableClient = storageAccount.createCloudTableClient();

            // Create the table if it doesn't exist.
            String tableName = "people";
            CloudTable cloudTable = tableClient.getTableReference(tableName);
            boolean flag = cloudTable.createIfNotExists();
            System.out.println(flag);
        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
        }
    }

    public void listTables() {
        try {
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount =
                    CloudStorageAccount.parse(storageConnectionString);

            CloudStorageAccount.getDevelopmentStorageAccount();

            // Create the table client.
            CloudTableClient tableClient = storageAccount.createCloudTableClient();

            // Loop through the collection of table names.
            for (String table : tableClient.listTables()) {
                // Output each table name.
                System.out.println(table);
            }
        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
        }
    }

    public void insertTable() {
        try {
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount =
                    CloudStorageAccount.parse(storageConnectionString);

            CloudStorageAccount.getDevelopmentStorageAccount();

            // Create the table client.
            CloudTableClient tableClient = storageAccount.createCloudTableClient();

            // Create a cloud table object for the table.
            CloudTable cloudTable = tableClient.getTableReference("people");

            // Create a new customer entity.
            CustomerEntity customer1 = new CustomerEntity("Harp", "Walter");
            customer1.setEmail("Walter@contoso.com");
            customer1.setPhoneNumber("425-555-0101");

            // Create an operation to add the new customer to the people table.
            TableOperation insertCustomer1 = TableOperation.insertOrReplace(customer1);

            // Submit the operation to the table service.
            System.out.println(cloudTable.execute(insertCustomer1));
        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
        }
    }

    public void insertBatchTable() {
        try {
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount =
                    CloudStorageAccount.parse(storageConnectionString);

            CloudStorageAccount.getDevelopmentStorageAccount();

            // Create the table client.
            CloudTableClient tableClient = storageAccount.createCloudTableClient();

            // Define a batch operation.
            TableBatchOperation batchOperation = new TableBatchOperation();

            // Create a cloud table object for the table.
            CloudTable cloudTable = tableClient.getTableReference("peoples");

            // Create a customer entity to add to the table.
            CustomerEntity customer = new CustomerEntity("Smith", "Jeff");
            customer.setEmail("Jeff@contoso.com");
            customer.setPhoneNumber("425-555-0104");
            batchOperation.insertOrReplace(customer);

            // Create another customer entity to add to the table.
            CustomerEntity customer2 = new CustomerEntity("Smith", "Ben");
            customer2.setEmail("Ben@contoso.com");
            customer2.setPhoneNumber("425-555-0102");
            batchOperation.insertOrReplace(customer2);

            // Create a third customer entity to add to the table.
            CustomerEntity customer3 = new CustomerEntity("Smith", "Denise");
            customer3.setEmail("Denise@contoso.com");
            customer3.setPhoneNumber("425-555-0103");
            batchOperation.insertOrReplace(customer3);

            // Execute the batch of operations on the "people" table.
            cloudTable.execute(batchOperation);
        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
        }
    }

    public void retrieveAllEntitiesTable() {
        try {
            // Define constants for filters.
            final String PARTITION_KEY = "PartitionKey";
            final String ROW_KEY = "RowKey";
            final String TIMESTAMP = "Timestamp";

            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount =
                    CloudStorageAccount.parse(storageConnectionString);

            CloudStorageAccount.getDevelopmentStorageAccount();

            // Create the table client.
            CloudTableClient tableClient = storageAccount.createCloudTableClient();

            // Create a cloud table object for the table.
            CloudTable cloudTable = tableClient.getTableReference("peoples");

            // Create a filter condition where the partition key is "Smith".
            String partitionFilter = TableQuery.generateFilterCondition(
                    PARTITION_KEY,
                    QueryComparisons.EQUAL,
                    "Smith");

            // Specify a partition query, using "Smith" as the partition key filter.
            TableQuery<CustomerEntity> partitionQuery =
                    TableQuery.from(CustomerEntity.class)
                            .where(partitionFilter);

            // Loop through the results, displaying information about the entity.
            for (CustomerEntity entity : cloudTable.execute(partitionQuery)) {
                System.out.println(entity.getPartitionKey() +
                        " " + entity.getRowKey() +
                        "\t" + entity.getEmail() +
                        "\t" + entity.getPhoneNumber());
            }
        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
        }
    }

    public void retrieveRangeEntitiesTable() {
        try {
            // Define constants for filters.
            final String PARTITION_KEY = "PartitionKey";
            final String ROW_KEY = "RowKey";
            final String TIMESTAMP = "Timestamp";

            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount =
                    CloudStorageAccount.parse(storageConnectionString);

            CloudStorageAccount.getDevelopmentStorageAccount();

            // Create the table client.
            CloudTableClient tableClient = storageAccount.createCloudTableClient();

            // Create a cloud table object for the table.
            CloudTable cloudTable = tableClient.getTableReference("peoples");

            // Create a filter condition where the partition key is "Smith".
            String partitionFilter = TableQuery.generateFilterCondition(
                    PARTITION_KEY,
                    QueryComparisons.EQUAL,
                    "Smith");

            // Create a filter condition where the row key is less than the letter "E".
            String rowFilter = TableQuery.generateFilterCondition(
                    ROW_KEY,
                    QueryComparisons.LESS_THAN,
                    "E");

            // Combine the two conditions into a filter expression.
            String combinedFilter = TableQuery.combineFilters(partitionFilter,
                    Operators.AND, rowFilter);

            // Specify a range query, using "Smith" as the partition key,
            // with the row key being up to the letter "E".
            TableQuery<CustomerEntity> rangeQuery =
                    TableQuery.from(CustomerEntity.class)
                            .where(combinedFilter);

            // Loop through the results, displaying information about the entity
            for (CustomerEntity entity : cloudTable.execute(rangeQuery)) {
                System.out.println(entity.getPartitionKey() +
                        " " + entity.getRowKey() +
                        "\t" + entity.getEmail() +
                        "\t" + entity.getPhoneNumber());
            }
        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
        }
    }

    public void retrieveSingleEntityTable() {
        try {
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount =
                    CloudStorageAccount.parse(storageConnectionString);

            CloudStorageAccount.getDevelopmentStorageAccount();

            // Create the table client.
            CloudTableClient tableClient = storageAccount.createCloudTableClient();

            // Create a cloud table object for the table.
            CloudTable cloudTable = tableClient.getTableReference("peoples");

            // Retrieve the entity with partition key of "Smith" and row key of "Jeff"
            TableOperation retrieveSmithJeff =
                    TableOperation.retrieve("Smith", "Jeff", CustomerEntity.class);

            // Submit the operation to the table service and get the specific entity.
            CustomerEntity specificEntity =
                    cloudTable.execute(retrieveSmithJeff).getResultAsType();

            // Output the entity.
            if (specificEntity != null) {
                System.out.println(specificEntity.getPartitionKey() +
                        " " + specificEntity.getRowKey() +
                        "\t" + specificEntity.getEmail() +
                        "\t" + specificEntity.getPhoneNumber());
            }
        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
        }
    }

    public void modifyEntity() {
        try {
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount =
                    CloudStorageAccount.parse(storageConnectionString);

            CloudStorageAccount.getDevelopmentStorageAccount();

            // Create the table client.
            CloudTableClient tableClient = storageAccount.createCloudTableClient();

            // Create a cloud table object for the table.
            CloudTable cloudTable = tableClient.getTableReference("peoples");

            // Retrieve the entity with partition key of "Smith" and row key of "Jeff".
            TableOperation retrieveSmithJeff =
                    TableOperation.retrieve("Smith", "Jeff", CustomerEntity.class);

            // Submit the operation to the table service and get the specific entity.
            CustomerEntity specificEntity =
                    cloudTable.execute(retrieveSmithJeff).getResultAsType();

            // Specify a new phone number.
            specificEntity.setPhoneNumber("425-555-0105");

            // Create an operation to replace the entity.
            TableOperation replaceEntity = TableOperation.replace(specificEntity);

            // Submit the operation to the table service.
            cloudTable.execute(replaceEntity);
        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
        }
    }

    public void deleteEntity() {
        try {
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount =
                    CloudStorageAccount.parse(storageConnectionString);

            CloudStorageAccount.getDevelopmentStorageAccount();

            // Create the table client.
            CloudTableClient tableClient = storageAccount.createCloudTableClient();

            // Create a cloud table object for the table.
            CloudTable cloudTable = tableClient.getTableReference("peoples");

            // Create an operation to retrieve the entity with partition key of "Smith" and row key of "Jeff".
            TableOperation retrieveSmithJeff = TableOperation.retrieve("Smith", "Jeff", CustomerEntity.class);

            // Retrieve the entity with partition key of "Smith" and row key of "Jeff".
            CustomerEntity entitySmithJeff =
                    cloudTable.execute(retrieveSmithJeff).getResultAsType();

            // Create an operation to delete the entity.
            TableOperation deleteSmithJeff = TableOperation.delete(entitySmithJeff);

            // Submit the delete operation to the table service.
            cloudTable.execute(deleteSmithJeff);
        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
        }
    }

    public void deleteTable() {
        try {
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount =
                    CloudStorageAccount.parse(storageConnectionString);

            CloudStorageAccount.getDevelopmentStorageAccount();

            // Create the table client.
            CloudTableClient tableClient = storageAccount.createCloudTableClient();

            // Delete the table and all its data if it exists.
            CloudTable cloudTable = tableClient.getTableReference("people");
            cloudTable.deleteIfExists();
        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
        }
    }
}
