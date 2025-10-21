#ifndef BLACKSCHOLES_H
#define BLACKSCHOLES_H

/**
 * Black-Scholes Option Pricing Model
 * 
 * This header defines the core Black-Scholes functions for option pricing
 * and Greeks calculation.
 */

/**
 * Cumulative distribution function for standard normal distribution
 */
double norm_cdf(double x);

/**
 * Probability density function for standard normal distribution
 */
double norm_pdf(double x);

/**
 * Calculate option price using Black-Scholes model
 * 
 * @param s Underlying asset price
 * @param k Strike price
 * @param r Risk-free interest rate
 * @param t Time to expiry (in years)
 * @param vol Volatility
 * @param is_call true for call option, false for put option
 * @param is_stock true to return stock price (ignores other params)
 * @return Option price
 */
double price(double s, double k, double r, double t, double vol, bool is_call, bool is_stock);

/**
 * Calculate delta (rate of change of option price with respect to underlying price)
 */
double delta(double s, double k, double r, double t, double vol, bool is_call, bool is_stock);

/**
 * Calculate gamma (rate of change of delta with respect to underlying price)
 */
double gamma(double s, double k, double r, double t, double vol, bool is_stock);

/**
 * Calculate theta (rate of change of option price with respect to time)
 */
double theta(double s, double k, double r, double t, double vol, bool is_call, bool is_stock);

/**
 * Calculate vega (rate of change of option price with respect to volatility)
 */
double vega(double s, double k, double r, double t, double vol, bool is_stock);

/**
 * Calculate rho (rate of change of option price with respect to interest rate)
 */
double rho(double s, double k, double r, double t, double vol, bool is_call, bool is_stock);

#endif // BLACKSCHOLES_H
