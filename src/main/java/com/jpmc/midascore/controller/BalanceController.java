package com.jpmc.midascore.controller;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Balance;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BalanceController {

    private final DatabaseConduit dbConduit;

    public BalanceController(DatabaseConduit dbConduit) {
        this.dbConduit = dbConduit;
    }

    @GetMapping("/balance")
    public Balance getBalance(@RequestParam (value = "userId", required = true) Long userId) {
        UserRecord user = dbConduit.getUserById(userId);

        // if user exists return balance, otherwise return 0
        if (user != null) {
            return new Balance(user.getBalance());
        } else {
            return new Balance(0f);
        }
    }
}
