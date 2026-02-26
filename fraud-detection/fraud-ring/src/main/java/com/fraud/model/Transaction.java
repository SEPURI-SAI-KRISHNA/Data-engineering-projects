package com.fraud.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Transaction {
    public String transactionId;
    public String senderAccount;
    public String receiverAccount;
    public double amount;
    public long timestamp;

    @Override
    public String toString() {
        return senderAccount + "->" + receiverAccount + " ($" + amount + ")";
    }
}