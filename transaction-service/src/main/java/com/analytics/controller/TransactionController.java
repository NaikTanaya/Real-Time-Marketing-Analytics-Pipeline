package com.analytics.controller;

import com.analytics.model.Transaction;
import com.analytics.service.TransactionService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/transactions")
public class TransactionController {

    private final TransactionService service;

    public TransactionController(TransactionService service) {
        this.service = service;
    }

    @PostMapping("/submit")
    public ResponseEntity<Transaction> submitTransaction(@RequestBody Transaction transaction) {
        return ResponseEntity.ok(service.processTransaction(transaction));
    }

    @GetMapping("/{id}/status")
    public ResponseEntity<String> getStatus(@PathVariable String id) {
        return ResponseEntity.ok(service.getTransactionStatus(id));
    }
}
