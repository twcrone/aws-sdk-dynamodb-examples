package com.twcrone.dynamodb;

import com.newrelic.api.agent.Trace;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbAsyncWaiter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class CustomerAsyncRepository {
    private final static String CUSTOMER_TABLE = "my_customers";
    private final static String KEY = "customerId";

    private final DynamoDbAsyncClient client;

    public CustomerAsyncRepository(DynamoDbAsyncClient client) {
        this.client = client;
    }

    public void createTableIfNeeded() throws ExecutionException, InterruptedException {
        ListTablesRequest request = ListTablesRequest.builder().build();
        CompletableFuture<ListTablesResponse> listTableResponse = client.listTables(request);

        CompletableFuture<CreateTableResponse> createTableRequest = listTableResponse
                .thenCompose(response -> {
                    boolean tableExist = response.tableNames().contains(CUSTOMER_TABLE);
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
        DescribeTableRequest tableRequest = DescribeTableRequest.builder().tableName(CUSTOMER_TABLE).build();
        DynamoDbAsyncWaiter dbWaiter = client.waiter();
        CompletableFuture<WaiterResponse<DescribeTableResponse>> waiterResponse =
                dbWaiter.waitUntilTableExists(tableRequest);

        WaiterResponse<DescribeTableResponse> response = waiterResponse.get();
        response.matched().response().ifPresent(System.out::println);
    }

    public void getItem(String uuid) {

        HashMap<String, AttributeValue> keyToGet =
                new HashMap<String, AttributeValue>();

        keyToGet.put(KEY, AttributeValue.builder()
                .s(uuid).build());

        try {

            // Create a GetItemRequest instance
            GetItemRequest request = GetItemRequest.builder()
                    .key(keyToGet)
                    .tableName(CUSTOMER_TABLE)
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

    private CompletableFuture<CreateTableResponse> createTable(DynamoDbAsyncClient client) {

        CreateTableRequest request = CreateTableRequest.builder()
                .tableName(CUSTOMER_TABLE)
                .keySchema(KeySchemaElement.builder().attributeName(KEY).keyType(KeyType.HASH).build())
                .attributeDefinitions(AttributeDefinition.builder().attributeName(KEY).attributeType(ScalarAttributeType.S).build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build();

        return client.createTable(request);
    }

    public void deleteTable() {
        DeleteTableRequest request = DeleteTableRequest.builder()
                .tableName(CUSTOMER_TABLE)
                .build();
        try {
            CompletableFuture<DeleteTableResponse> future = client.deleteTable(request);
            DeleteTableResponse response = future.get();
        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Trace(dispatcher = true)
    public Flux<Customer> listCustomers() {
        System.out.println("Getting all customers...");
        ScanRequest scanRequest = ScanRequest.builder()
                .tableName(CUSTOMER_TABLE)
                .build();

        return Mono.fromCompletionStage(client.scan(scanRequest))
                .map(ScanResponse::items)
                .map(CustomerMapper::fromList)
                .flatMapMany(Flux::fromIterable);
    }

    @Trace(dispatcher = true)
    public Mono<Customer> createCustomer(Customer customer) {
        System.out.println("Creating " + customer.getName());
        customer.setId(UUID.randomUUID().toString());

        PutItemRequest putItemRequest = PutItemRequest.builder()
                .tableName(CUSTOMER_TABLE)
                .item(CustomerMapper.toMap(customer))
                .build();

        return Mono.fromCompletionStage(client.putItem(putItemRequest))
                .map(PutItemResponse::attributes)
                .map(attributeValueMap -> customer);
    }

    @Trace(dispatcher = true)
    public Mono<String> deleteCustomer(String customerId) {
        System.out.println("Deleting " + customerId);
        DeleteItemRequest deleteItemRequest = DeleteItemRequest.builder()
                .tableName(CUSTOMER_TABLE)
                .key(Map.of("customerId", AttributeValue.builder().s(customerId).build()))
                .build();

        return Mono.fromCompletionStage(client.deleteItem(deleteItemRequest))
                .map(DeleteItemResponse::attributes)
                .map(attributeValueMap -> customerId);
    }

    @Trace(dispatcher = true)
    public Mono<Customer> getCustomer(String customerId) {
        System.out.println("Fetching " + customerId);
        GetItemRequest getItemRequest = GetItemRequest.builder()
                .tableName(CUSTOMER_TABLE)
                .key(Map.of("customerId", AttributeValue.builder().s(customerId).build()))
                .build();

        return Mono.fromCompletionStage(client.getItem(getItemRequest))
                .map(GetItemResponse::item)
                .map(CustomerMapper::fromMap);
    }

    @Trace(dispatcher = true)
    public Mono<String> updateCustomer(Customer customer) {
        System.out.println("Updating " + customer.getName());
        PutItemRequest putItemRequest = PutItemRequest.builder()
                .tableName(CUSTOMER_TABLE)
                .item(CustomerMapper.toMap(customer))
                .build();

        return Mono.fromCompletionStage(client.putItem(putItemRequest))
                .map(updateItemResponse -> customer.getId());
    }
}