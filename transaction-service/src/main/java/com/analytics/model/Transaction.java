package com.analytics.model;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "transactions")
public class Transaction {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String transactionId;

    private String customerId;
    private double amount;
    private String status; // SUCCESS, FAILED, PENDING
    private String routingGateway; // e.g., GATEWAY_PRIMARY, GATEWAY_SECONDARY
    private double latencyMs;
    private LocalDateTime timestamp;

    public Transaction() {}

    public Transaction(String customerId, double amount, String status, String routingGateway, double latencyMs) {
        this.customerId = customerId;
        this.amount = amount;
        this.status = status;
        this.routingGateway = routingGateway;
        this.latencyMs = latencyMs;
        this.timestamp = LocalDateTime.now();
    }

    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }

    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String getRoutingGateway() { return routingGateway; }
    public void setRoutingGateway(String routingGateway) { this.routingGateway = routingGateway; }

    public double getLatencyMs() { return latencyMs; }
    public void setLatencyMs(double latencyMs) { this.latencyMs = latencyMs; }

    public LocalDateTime getTimestamp() { return timestamp; }
    public void setTimestamp(LocalDateTime timestamp) { this.timestamp = timestamp; }
}
