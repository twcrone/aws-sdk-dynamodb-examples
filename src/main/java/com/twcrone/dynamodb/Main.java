package com.twcrone.dynamodb;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
// snippet-end:[dynamoasyn.java2.get_item.import]

public class Main {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        DynamoDbAsyncClient client = DynamoDbAsyncClient.builder()
                .region(Region.US_EAST_1)
                .build();
        CustomerRepository repository = new CustomerRepository(client);
        System.out.println("*** Creating table if needed ***");
        repository.createTableIfNeeded();
        for (int i = 0; i < 100; i++) {
            new AwsTransactions(repository).run();
            Thread.sleep(3000);
        }
        System.out.println("*** Deleting table ***");
        repository.deleteTable();
    }
}

// snippet-end:[dynamodb.Java.DynamoDBAsyncGetItem.complete]