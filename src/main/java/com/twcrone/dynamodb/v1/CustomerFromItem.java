package com.twcrone.dynamodb.v1;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.twcrone.dynamodb.Customer;

import java.util.Map;

public class CustomerFromItem {
    public static Customer from(Map<String, AttributeValue> item) {
        if (item == null) {
            return null;
        }
        String id = item.get("id").getS();
        String name = item.get("name").getS();
        String city = item.get("city").getS();
        String email = item.get("email").getS();
        Customer customer = new Customer(name, email, city);
        customer.setId(id);
        return customer;
    }
}
