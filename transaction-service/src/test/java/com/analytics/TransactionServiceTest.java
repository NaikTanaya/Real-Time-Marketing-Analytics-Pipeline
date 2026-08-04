package com.analytics;

import com.analytics.model.Transaction;
import com.analytics.repository.TransactionRepository;
import com.analytics.service.TransactionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;

class TransactionServiceTest {

    private TransactionRepository repository;
    private StringRedisTemplate redisTemplate;
    private ValueOperations<String, String> valueOperations;
    private TransactionService transactionService;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        repository = Mockito.mock(TransactionRepository.class);
        redisTemplate = Mockito.mock(StringRedisTemplate.class);
        valueOperations = Mockito.mock(ValueOperations.class);

        Mockito.when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        transactionService = new TransactionService(repository, redisTemplate);
    }

    @Test
    void testProcessTransactionWithDefaultGateway() {
        Transaction tx = new Transaction("CUST_100", 150.0, null, null, 0.0);
        Mockito.when(valueOperations.get("ROUTING_RULE:PREFERRED_GATEWAY")).thenReturn(null);
        Mockito.when(repository.save(any(Transaction.class))).thenAnswer(i -> i.getArguments()[0]);

        Transaction processed = transactionService.processTransaction(tx);

        assertNotNull(processed);
        assertEquals("GATEWAY_PRIMARY", processed.getRoutingGateway());
        assertEquals("SUCCESS", processed.getStatus());
    }
}
