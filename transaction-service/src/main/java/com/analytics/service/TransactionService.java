package com.analytics.service;

import com.analytics.model.Transaction;
import com.analytics.repository.TransactionRepository;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Optional;

@Service
public class TransactionService {

    private final TransactionRepository repository;
    private final StringRedisTemplate redisTemplate;

    public TransactionService(TransactionRepository repository, StringRedisTemplate redisTemplate) {
        this.repository = repository;
        this.redisTemplate = redisTemplate;
    }

    public Transaction processTransaction(Transaction tx) {
        long startTime = System.currentTimeMillis();

        // 1. Consult Redis for the current metric-driven dynamic routing rule
        String optimalGateway = Optional.ofNullable(redisTemplate.opsForValue().get("ROUTING_RULE:PREFERRED_GATEWAY"))
                .orElse("GATEWAY_PRIMARY");

        tx.setRoutingGateway(optimalGateway);
        tx.setStatus("SUCCESS");
        tx.setTimestamp(LocalDateTime.now());

        long executionTime = System.currentTimeMillis() - startTime;
        tx.setLatencyMs((double) executionTime);

        // 2. Persist transaction details in PostgreSQL
        Transaction savedTransaction = repository.save(tx);

        // 3. Cache status in Redis for rapid read access
        redisTemplate.opsForValue().set("TX_STATUS:" + savedTransaction.getTransactionId(), savedTransaction.getStatus());

        return savedTransaction;
    }

    public String getTransactionStatus(String id) {
        String cachedStatus = redisTemplate.opsForValue().get("TX_STATUS:" + id);
        if (cachedStatus != null) return cachedStatus;

        return repository.findById(id)
                .map(Transaction::getStatus)
                .orElse("NOT_FOUND");
    }
}
