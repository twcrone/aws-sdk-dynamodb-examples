package com.twcrone.dynamodb.sync;

import com.newrelic.api.agent.Trace;
import com.twcrone.dynamodb.Customer;
import com.twcrone.dynamodb.CustomerMapper;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbAsyncWaiter;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class CustomerRepository {
    private final static String CUSTOMER_TABLE = "my_customers";
    private final static String KEY = "customerId";

    private final DynamoDbClient client;

    public CustomerRepository(DynamoDbClient client) {
        this.client = client;
    }

    @Trace(dispatcher = true)
    public void createTableIfNeeded() throws ExecutionException, InterruptedException {
        ListTablesRequest request = ListTablesRequest.builder().build();
        ListTablesResponse listTableResponse = client.listTables(request);

        if(!listTableResponse.tableNames().contains(CUSTOMER_TABLE)) {
            CreateTableResponse createTableResponse = createTable();
            System.out.println(createTableResponse.tableDescription());

            // Wait for full table creation on AWS before returning
            DescribeTableRequest tableRequest = DescribeTableRequest.builder().tableName(CUSTOMER_TABLE).build();
            DynamoDbWaiter dbWaiter = client.waiter();
            WaiterResponse<DescribeTableResponse> waiterResponse =
                    dbWaiter.waitUntilTableExists(tableRequest);
            waiterResponse.matched().response().ifPresent(System.out::println);
        }

    }

    @Trace(dispatcher = true)
    private CreateTableResponse createTable() {

        CreateTableRequest request = CreateTableRequest.builder()
                .tableName(CUSTOMER_TABLE)
                .keySchema(KeySchemaElement.builder().attributeName(KEY).keyType(KeyType.HASH).build())
                .attributeDefinitions(AttributeDefinition.builder().attributeName(KEY).attributeType(ScalarAttributeType.S).build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build();

        return client.createTable(request);
    }

    @Trace(dispatcher = true)
    public void deleteTable() {
        DeleteTableRequest request = DeleteTableRequest.builder()
                .tableName(CUSTOMER_TABLE)
                .build();
        try {
            DeleteTableResponse response = client.deleteTable(request);
        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
        }
    }

    @Trace(dispatcher = true)
    public List<Customer> listCustomers() {
        System.out.println("Getting all customers...");
        ScanRequest scanRequest = ScanRequest.builder()
                .tableName(CUSTOMER_TABLE)
                .build();

        return CustomerMapper.fromList(client.scan(scanRequest).items());
    }

//    @Trace(dispatcher = true)
//    public Mono<Customer> createCustomer(Customer customer) {
//        System.out.println("Creating " + customer.getName());
//        customer.setId(UUID.randomUUID().toString());
//
//        PutItemRequest putItemRequest = PutItemRequest.builder()
//                .tableName(CUSTOMER_TABLE)
//                .item(CustomerMapper.toMap(customer))
//                .build();
//
//        return Mono.fromCompletionStage(client.putItem(putItemRequest))
//                .map(PutItemResponse::attributes)
//                .map(attributeValueMap -> customer);
//    }
//
//    @Trace(dispatcher = true)
//    public Mono<String> deleteCustomer(String customerId) {
//        System.out.println("Deleting " + customerId);
//        DeleteItemRequest deleteItemRequest = DeleteItemRequest.builder()
//                .tableName(CUSTOMER_TABLE)
//                .key(Map.of("customerId", AttributeValue.builder().s(customerId).build()))
//                .build();
//
//        return Mono.fromCompletionStage(client.deleteItem(deleteItemRequest))
//                .map(DeleteItemResponse::attributes)
//                .map(attributeValueMap -> customerId);
//    }
//
//    @Trace(dispatcher = true)
//    public Mono<Customer> getCustomer(String customerId) {
//        System.out.println("Fetching " + customerId);
//        GetItemRequest getItemRequest = GetItemRequest.builder()
//                .tableName(CUSTOMER_TABLE)
//                .key(Map.of("customerId", AttributeValue.builder().s(customerId).build()))
//                .build();
//
//        return Mono.fromCompletionStage(client.getItem(getItemRequest))
//                .map(GetItemResponse::item)
//                .map(CustomerMapper::fromMap);
//    }
//
//    @Trace(dispatcher = true)
//    public Mono<String> updateCustomer(Customer customer) {
//        System.out.println("Updating " + customer.getName());
//        PutItemRequest putItemRequest = PutItemRequest.builder()
//                .tableName(CUSTOMER_TABLE)
//                .item(CustomerMapper.toMap(customer))
//                .build();
//
//        return Mono.fromCompletionStage(client.putItem(putItemRequest))
//                .map(updateItemResponse -> customer.getId());
//    }
}