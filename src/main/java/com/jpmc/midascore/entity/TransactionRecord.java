package com.jpmc.midascore.entity;

import jakarta.persistence.*;

@Entity
public class TransactionRecord {

    @Id
    @GeneratedValue
    private Long Id;

    @ManyToOne(optional = false)
    private UserRecord sender;

    @ManyToOne(optional = false)
    private UserRecord recipient;

    @Column(nullable = false)
    private float amount;

    @Column(nullable = false)
    private float incentive;

    protected TransactionRecord(UserRecord sender, UserRecord recipient, float amount) {
    }

    public TransactionRecord(UserRecord sender, UserRecord recipient, float amount, float incentive) {
        this.sender = sender;
        this.recipient = recipient;
        this.amount = amount;
        this.incentive = incentive;
    }

    public Long getId() {
        return Id;
    }

    public UserRecord getSender() {
        return sender;
    }

    public UserRecord getRecipient() {
        return recipient;
    }

    public float getIncentive() {
        return incentive;
    }

    public float getAmount() {
        return amount;
    }
}