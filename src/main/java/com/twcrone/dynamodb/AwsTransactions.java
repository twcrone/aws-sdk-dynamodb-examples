package com.twcrone.dynamodb;

import com.newrelic.api.agent.Trace;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class AwsTransactions {
    private final CustomerRepository repository;

    public AwsTransactions(CustomerRepository customerRepository) {
        this.repository = customerRepository;
    }

    @Trace(dispatcher = true)
    public void run() {
        try {
            //System.out.format("Retrieving item \"%s\" from \"%s\"\n", keyVal, tableName );
            repository.getItem("ID");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
