package com.twcrone.dynamodb;

import com.twcrone.dynamodb.v1.V1SyncCustomerRepository;
import com.twcrone.dynamodb.v2.V2SyncCustomerRepository;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.concurrent.ExecutionException;

public class Main {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.out.println("*** V1 Sync Transactions ");
        new AwsTransactions(new V1SyncCustomerRepository()).run();

        System.out.println("Waiting to make room for next simulations...");
        Thread.sleep(30000);

        System.out.println("*** V2 Sync Transactions ");
        new AwsTransactions(new V2SyncCustomerRepository()).run();
    }
}