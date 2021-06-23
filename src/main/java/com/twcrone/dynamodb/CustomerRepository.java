package com.twcrone.dynamodb;

import com.newrelic.api.agent.Trace;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbAsyncWaiter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class CustomerRepository {
    private final String TABLE_NAME = "customers";
    private final DynamoDbAsyncClient client;

    public CustomerRepository(DynamoDbAsyncClient client) {
        this.client = client;
    }

    public void createTableIfNeeded() throws ExecutionException, InterruptedException {
        ListTablesRequest request = ListTablesRequest.builder().build();
        CompletableFuture<ListTablesResponse> listTableResponse = client.listTables(request);

        CompletableFuture<CreateTableResponse> createTableRequest = listTableResponse
                .thenCompose(response -> {
                    boolean tableExist = response.tableNames().contains("customers");
                    if (!tableExist) {
                        return createTable(client);
                    } else {
                        System.out.println("Table already exists!");
                        return CompletableFuture.completedFuture(null);
                    }
                });

        // Wait in synchronous manner for table creation
        createTableRequest.get();

        // Wait for full table creation on AWS before returning
        DescribeTableRequest tableRequest = DescribeTableRequest.builder().tableName(TABLE_NAME).build();
        DynamoDbAsyncWaiter dbWaiter = client.waiter();
        CompletableFuture<WaiterResponse<DescribeTableResponse>> waiterResponse =
                dbWaiter.waitUntilTableExists(tableRequest);

        WaiterResponse<DescribeTableResponse> response = waiterResponse.get();
        response.matched().response().ifPresent(System.out::println);
    }

    @Trace(dispatcher = true)
    public void getItem(String uuid) {

        HashMap<String, AttributeValue> keyToGet =
                new HashMap<String, AttributeValue>();

        keyToGet.put("customerId", AttributeValue.builder()
                .s(uuid).build());

        try {

            // Create a GetItemRequest instance
            GetItemRequest request = GetItemRequest.builder()
                    .key(keyToGet)
                    .tableName(TABLE_NAME)
                    .build();

            // Invoke the DynamoDbAsyncClient object's getItem
            java.util.Collection<software.amazon.awssdk.services.dynamodb.model.AttributeValue> returnedItem = client.getItem(request).join().item().values();

            // Convert Set to Map
            Map<String, AttributeValue> map = returnedItem.stream().collect(Collectors.toMap(AttributeValue::s, s->s));
            Set<String> keys = map.keySet();
            for (String sinKey : keys) {
                System.out.format("%s: %s\n", sinKey, map.get(sinKey).toString());
            }

        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        // snippet-end:[dynamoasyc.java2.get_item.main]
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