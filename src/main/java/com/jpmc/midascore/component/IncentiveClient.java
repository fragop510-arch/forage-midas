package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class IncentiveClient {

    private final RestTemplate restTemplate = new RestTemplate();

    private static final String URL = "http://localhost:8080/incentive";

    public Incentive getIncentive(Transaction transaction) {
        return restTemplate.postForObject(
                URL,
                transaction,
                Incentive.class
        );
    }
}