package com.twcrone.dynamodb;

import com.twcrone.dynamodb.sync.CustomerRepository;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.concurrent.ExecutionException;

public class Main {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        DynamoDbClient client = DynamoDbClient.builder()
                .region(Region.US_EAST_1)
                .build();
        CustomerRepository repository = new CustomerRepository(client);
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