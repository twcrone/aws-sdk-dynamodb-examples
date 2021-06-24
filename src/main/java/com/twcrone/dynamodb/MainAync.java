package com.twcrone.dynamodb;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import java.util.concurrent.ExecutionException;

public class MainAync {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        DynamoDbAsyncClient client = DynamoDbAsyncClient.builder()
                .region(Region.US_EAST_1)
                .build();
        CustomerAsyncRepository repository = new CustomerAsyncRepository(client);
        System.out.println("*** Creating table if needed ***");
        repository.createTableIfNeeded();
        for (int i = 0; i < 25; i++) {
            new AwsTransactions(repository).run();
            System.out.println("Run " + (i + 1) + " completed");
            Thread.sleep(3000);
        }
        System.out.println("*** Deleting table ***");
        repository.deleteTable();
    }
}