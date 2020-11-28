package com.github.michaelheinecke.kafkastreams.bankbalance;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public class Transaction {
    @JsonProperty
    public String name;

    @JsonProperty
    public Integer amount;

    @JsonProperty
    public String timestamp;

    public Transaction(String name, Integer amount, String timestamp) {
        this.name = name;
        this.amount = amount;
        this.timestamp = timestamp;
    }
}
