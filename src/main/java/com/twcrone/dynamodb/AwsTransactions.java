package com.twcrone.dynamodb;

import com.newrelic.api.agent.Trace;
import reactor.core.publisher.Flux;

import java.util.List;

public class AwsTransactions {
    private final CustomerRepository repository;

    public AwsTransactions(CustomerRepository customerRepository) {
        this.repository = customerRepository;
    }

    public void run() {
        try {
            //System.out.format("Retrieving item \"%s\" from \"%s\"\n", keyVal, tableName );
            System.out.println("Processing java devs...");
            List<String> ids = Flux.just(
                    new Customer("Jason", "jason@nr.com", "Portland"),
                    new Customer("Xi Xia", "xixia@nr.com", "Portland"),
                    new Customer("Brad", "brad@nr.com", "Portland"),
                    new Customer("André", "andre@nr.com", "Virginia"),
                    new Customer("Kevyn", "kevyn@nr.com", "Jersey"),
                    new Customer("Chinmay", "chinmay@nr.com", "Berkley"),
                    new Customer("Todd", "todd@nr.com", "Triangle"))
                    .flatMap(repository::createCustomer)
                    .flatMap((dev) -> repository.getCustomer(dev.getId()))
                    .map(AwsTransactions::moveToBarcelona)
                    .flatMap(repository::updateCustomer)
                    .flatMap(repository::deleteCustomer)
                    .collectList().block();

            repository.listCustomers()
                    .doOnEach(System.out::println)
                    .collectList().block();

            System.out.println(ids);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Customer moveToBarcelona(Customer customer) {
        System.out.println("Moving " + customer.getName() + " to Barcelona");
        customer.setCity("Barcelona");
        return customer;
    }
}
