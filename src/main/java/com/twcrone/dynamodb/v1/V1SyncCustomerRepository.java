package com.twcrone.dynamodb.v1;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.*;
import com.amazonaws.services.dynamodbv2.model.*;
import com.newrelic.api.agent.Trace;
import com.twcrone.dynamodb.Customer;
import com.twcrone.dynamodb.CustomerRepository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class V1SyncCustomerRepository implements CustomerRepository {
    private final static String TABLE_NAME = "my_customers";
    private final static String KEY = "customerId";

    private final AmazonDynamoDB client;

    public V1SyncCustomerRepository() {
        this.client = AmazonDynamoDBClient.builder()
                .withRegion(Regions.US_EAST_1)
                .build();
    }

    @Trace(dispatcher = true)
    public void createTableIfNeeded() {
        if (tableExists()) {
            return;
        }
        createTable();
        waitForTable();
    }

    private void waitForTable() {
        try {
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private boolean tableExists() {
        List<String> tableNames = client.listTables().getTableNames();
        return tableNames.contains(TABLE_NAME);
    }

    @Trace(dispatcher = true)
    public void createTable() {
        CreateTableRequest request = new CreateTableRequest()
                .withAttributeDefinitions(new AttributeDefinition(
                        KEY, ScalarAttributeType.S))
                .withKeySchema(new KeySchemaElement(KEY, KeyType.HASH))
                .withProvisionedThroughput(new ProvisionedThroughput(
                        1L, 1L))
                .withTableName(TABLE_NAME);

        try {
            CreateTableResult result = client.createTable(request);
            System.out.println(result.getTableDescription().getTableName());
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
        }
    }

    @Trace(dispatcher = true)
    public void deleteTable() {
        try {
            client.deleteTable(TABLE_NAME);
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
    }

    @Trace(dispatcher = true)
    public List<Customer> listCustomers() {
        System.out.println("Getting all customers...");
        ScanRequest scanRequest = new ScanRequest(TABLE_NAME);
        return client.scan(scanRequest).getItems().stream()
                .map(CustomerFromItem::from)
                .collect(Collectors.toList());
    }

    @Trace(dispatcher = true)
    public Customer createCustomer(Customer customer) {
        Map<String,AttributeValue> itemValues = new HashMap<>();
        String id = UUID.randomUUID().toString();
        itemValues.put(KEY, new AttributeValue(id));
        itemValues.put("name", new AttributeValue(customer.getName()));
        itemValues.put("email", new AttributeValue(customer.getEmail()));
        itemValues.put("city", new AttributeValue(customer.getCity()));

        try {
            client.putItem(TABLE_NAME, itemValues);
        } catch (ResourceNotFoundException e) {
            System.err.format("Error: The table \"%s\" can't be found.\n", TABLE_NAME);
            System.err.println("Be sure that it exists and that you've typed its name correctly!");
        } catch (AmazonServiceException e) {
            System.err.println(e.getMessage());
        }
        customer.setId(id);
        return customer;
    }

    @Override
    public Customer getCustomer(String id) {
        return null;
    }

    @Override
    public Customer updateCustomer(Customer customer) {
        return null;
    }

    @Override
    public Customer deleteCustomer(String id) {
        return null;
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