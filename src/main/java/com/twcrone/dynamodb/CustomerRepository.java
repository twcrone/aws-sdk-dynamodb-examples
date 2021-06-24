package com.twcrone.dynamodb;

import java.util.List;

public interface CustomerRepository {
    void createTableIfNeeded();
    void createTable();
    void deleteTable();
    List<Customer> listCustomers();
    Customer createCustomer(Customer customer);
    Customer getCustomer(String id);
    Customer updateCustomer(Customer customer);
    Customer deleteCustomer(String id);
}