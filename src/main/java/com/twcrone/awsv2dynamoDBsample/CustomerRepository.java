package com.twcrone.awsv2dynamoDBsample;

import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbAsyncWaiter;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class CustomerRepository {
    private final String TABLE_NAME = "customers";
    private final DynamoDbAsyncClient client;

    public CustomerRepository(DynamoDbAsyncClient client) {
        this.client = client;
    }

    public String createTableIfNeeded() throws ExecutionException, InterruptedException {
        ListTablesRequest request = ListTablesRequest.builder().build();
        CompletableFuture<ListTablesResponse> listTableResponse = client.listTables(request);

        CompletableFuture<CreateTableResponse> createTableRequest = listTableResponse
                .thenCompose(response -> {
                    boolean tableExist = response.tableNames().contains("customers");
                    if (!tableExist) {
                        return createTable(client);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });

        // Wait in synchronous manner for table creation
        String tableName = createTableRequest.get().tableDescription().tableName();

        // Wait for full table creation on AWS before returning
        DescribeTableRequest tableRequest = DescribeTableRequest.builder().tableName(TABLE_NAME).build();
        DynamoDbAsyncWaiter dbWaiter = client.waiter();
        CompletableFuture<WaiterResponse<DescribeTableResponse>> waiterResponse =
                dbWaiter.waitUntilTableExists(tableRequest);

        WaiterResponse<DescribeTableResponse> response = waiterResponse.get();
        response.matched().response().ifPresent(System.out::println);

        return tableName;
    }

    private static CompletableFuture<CreateTableResponse> createTable(DynamoDbAsyncClient client) {

        CreateTableRequest request = CreateTableRequest.builder()
                .tableName("customers")

                .keySchema(KeySchemaElement.builder().attributeName("customerId").keyType(KeyType.HASH).build())
                .attributeDefinitions(AttributeDefinition.builder().attributeName("customerId").attributeType(ScalarAttributeType.S).build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build();

        return client.createTable(request);
    }

    public void deleteTable() {
        DeleteTableRequest request = DeleteTableRequest.builder()
                .tableName(TABLE_NAME)
                .build();
        try {
            CompletableFuture<DeleteTableResponse> future = client.deleteTable(request);
            DeleteTableResponse response = future.get();
        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}