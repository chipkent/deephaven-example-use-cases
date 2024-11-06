package org.example;

import io.deephaven.BlackScholes;

public class Main {

    public static void main(String[] args) {
        final double p = BlackScholes.price(100, 95, 0.05, 0.6, 0.4, true, false);
        System.out.println("Black-Scholes Price: " + p);
    }
}
