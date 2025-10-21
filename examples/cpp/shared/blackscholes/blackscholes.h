#ifndef BLACKSCHOLES_H
#define BLACKSCHOLES_H

/**
 * @file blackscholes.h
 * @brief Black-Scholes Option Pricing Model
 * 
 * This header defines the core Black-Scholes functions for European option pricing
 * and Greeks calculation. The Black-Scholes model is used to calculate the theoretical
 * price of options based on factors such as underlying asset price, strike price,
 * time to expiration, volatility, and risk-free interest rate.
 * 
 * The Greeks (delta, gamma, theta, vega, rho) measure the sensitivity of the option
 * price to changes in various parameters, which is essential for risk management
 * and hedging strategies.
 * 
 * This is a shared implementation used by both JavaCPP and pybind11 integration examples.
 */

/**
 * @brief Black-Scholes option pricing namespace
 * 
 * This namespace contains all Black-Scholes option pricing functions and utilities.
 * Using a namespace prevents naming conflicts with standard library functions (e.g., std::gamma).
 */
namespace BlackScholes {

/**
 * @brief Cumulative distribution function for standard normal distribution
 * 
 * Computes the probability that a standard normal random variable is less than or equal to x.
 * 
 * @param x Input value
 * @return Probability P(X <= x) where X ~ N(0,1)
 */
double norm_cdf(double x);

/**
 * @brief Probability density function for standard normal distribution
 * 
 * Computes the value of the standard normal probability density function at x.
 * 
 * @param x Input value
 * @return PDF value at x for N(0,1) distribution
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
 * @brief Calculate delta (∂V/∂S) - sensitivity to underlying price changes
 * 
 * Delta measures the rate of change of the option price with respect to changes
 * in the underlying asset price. For calls, delta ranges from 0 to 1; for puts,
 * from -1 to 0. Delta is also interpreted as the hedge ratio.
 * 
 * @param s Underlying asset price
 * @param k Strike price
 * @param r Risk-free interest rate (annualized)
 * @param t Time to expiry (in years)
 * @param vol Volatility (annualized)
 * @param is_call true for call option, false for put option
 * @param is_stock true to return 1.0 (stock delta), false for option delta
 * @return Delta value (typically between -1 and 1)
 */
double delta(double s, double k, double r, double t, double vol, bool is_call, bool is_stock);

/**
 * @brief Calculate gamma (∂²V/∂S²) - sensitivity of delta to underlying price changes
 * 
 * Gamma measures the rate of change of delta with respect to changes in the
 * underlying asset price. High gamma indicates that delta is very sensitive to
 * price movements. Gamma is highest for at-the-money options near expiration.
 * 
 * @param s Underlying asset price
 * @param k Strike price
 * @param r Risk-free interest rate (annualized)
 * @param t Time to expiry (in years)
 * @param vol Volatility (annualized)
 * @param is_stock true to return 0.0 (stock has no gamma), false for option gamma
 * @return Gamma value (always positive for long options)
 */
double gamma(double s, double k, double r, double t, double vol, bool is_stock);

/**
 * @brief Calculate theta (∂V/∂t) - sensitivity to time decay
 * 
 * Theta measures the rate of change of the option price with respect to the
 * passage of time (time decay). Theta is typically negative for long options,
 * indicating that options lose value as time passes, all else being equal.
 * 
 * @param s Underlying asset price
 * @param k Strike price
 * @param r Risk-free interest rate (annualized)
 * @param t Time to expiry (in years)
 * @param vol Volatility (annualized)
 * @param is_call true for call option, false for put option
 * @param is_stock true to return 0.0 (stock has no time decay), false for option theta
 * @return Theta value (typically negative for long options)
 */
double theta(double s, double k, double r, double t, double vol, bool is_call, bool is_stock);

/**
 * @brief Calculate vega (∂V/∂σ) - sensitivity to volatility changes
 * 
 * Vega measures the rate of change of the option price with respect to changes
 * in volatility. Vega is always positive for long options, meaning option prices
 * increase when volatility increases. Vega is highest for at-the-money options.
 * 
 * @param s Underlying asset price
 * @param k Strike price
 * @param r Risk-free interest rate (annualized)
 * @param t Time to expiry (in years)
 * @param vol Volatility (annualized)
 * @param is_stock true to return 0.0 (stock has no vega), false for option vega
 * @return Vega value (always positive for long options)
 */
double vega(double s, double k, double r, double t, double vol, bool is_stock);

/**
 * @brief Calculate rho (∂V/∂r) - sensitivity to interest rate changes
 * 
 * Rho measures the rate of change of the option price with respect to changes
 * in the risk-free interest rate. Call options have positive rho (benefit from
 * rate increases), while put options have negative rho. Rho is typically the
 * least significant Greek for short-term options.
 * 
 * @param s Underlying asset price
 * @param k Strike price
 * @param r Risk-free interest rate (annualized)
 * @param t Time to expiry (in years)
 * @param vol Volatility (annualized)
 * @param is_call true for call option (positive rho), false for put option (negative rho)
 * @param is_stock true to return 0.0 (stock has no rho), false for option rho
 * @return Rho value (scaled by 0.01 for 1% rate change)
 */
double rho(double s, double k, double r, double t, double vol, bool is_call, bool is_stock);

}  // namespace BlackScholes

#endif // BLACKSCHOLES_H
