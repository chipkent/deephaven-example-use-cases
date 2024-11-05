
""" Black-Scholes option pricing model in Python using Numba for vectorization """

import math
import numba
import numpy as np


@numba.vectorize(['float64(float64)'])
def norm_cdf(x):
    """ Cumulative distribution function for the standard normal distribution """
    return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0


@numba.vectorize(['float64(float64)'])
def norm_pdf(x):
    """ Probability density function for the standard normal distribution """
    return math.exp(-x**2 / 2.0) / math.sqrt(2.0 * math.pi)


@numba.vectorize(['float64(float64, float64, float64, float64, float64, boolean, boolean)'])
def black_scholes_price(s, k, r, t, vol, is_call, is_stock):
    """ Calculates the Black-Scholes option price for a call/put """

    if is_stock:
        return s

    d1 = (np.log(s / k) + (r + vol ** 2 / 2) * t) / (vol * np.sqrt(t))
    d2 = d1 - vol * np.sqrt(t)

    if is_call:
        return s * norm_cdf(d1) - k * np.exp(-r * t) * norm_cdf(d2)
    else:
        return k * np.exp(-r * t) * norm_cdf(-d2) - s * norm_cdf(-d1)


@numba.vectorize(['float64(float64, float64, float64, float64, float64, boolean, boolean)'])
def black_scholes_delta(s, k, r, t, vol, is_call, is_stock):
    """ Calculates the Black-Scholes option delta for a call/put """

    if is_stock:
        return 1.0

    d1 = (np.log(s / k) + (r + vol ** 2 / 2) * t) / (vol * np.sqrt(t))
    # d2 = d1 - vol * np.sqrt(T)

    if is_call:
        return norm_cdf(d1)
    else:
        return -norm_cdf(-d1)


@numba.vectorize(['float64(float64, float64, float64, float64, float64, boolean)'])
def black_scholes_gamma(s, k, r, t, vol, is_stock):
    """ Calculates the Black-Scholes option gamma """

    if is_stock:
        return 0.0

    d1 = (np.log(s / k) + (r + vol ** 2 / 2) * t) / (vol * np.sqrt(t))
    # d2 = d1 - vol * np.sqrt(T)

    return norm_pdf(d1) / (s * vol * np.sqrt(t))


@numba.vectorize(['float64(float64, float64, float64, float64, float64, boolean, boolean)'])
def black_scholes_theta(s, k, r, t, vol, is_call, is_stock):
    """ Calculates the Black-Scholes option theta """

    if is_stock:
        return 0.0

    d1 = (np.log(s / k) + (r + vol ** 2 / 2) * t) / (vol * np.sqrt(t))
    d2 = d1 - vol * np.sqrt(t)

    if is_call:
        return - ((s * norm_pdf(d1) * vol) / (2 * np.sqrt(t))) - r * k * np.exp(-r * t) * norm_cdf(d2)
    else:
        return - ((s * norm_pdf(d1) * vol) / (2 * np.sqrt(t))) + r * k * np.exp(-r * t) * norm_cdf(-d2)


@numba.vectorize(['float64(float64, float64, float64, float64, float64, boolean)'])
def black_scholes_vega(s, k, r, t, vol, is_stock):
    """ Calculates the Black-Scholes option vega """

    if is_stock:
        return 0.0

    d1 = (np.log(s / k) + (r + vol ** 2 / 2) * t) / (vol * np.sqrt(t))
    return s * np.sqrt(t) * norm_pdf(d1)


@numba.vectorize(['float64(float64, float64, float64, float64, float64, boolean, boolean)'])
def black_scholes_rho(s, k, r, t, vol, is_call, is_stock):
    """ Calculates the Black-Scholes option rho """

    if is_stock:
        return 0.0

    d1 = (np.log(s / k) + (r + vol ** 2 / 2) * t) / (vol * np.sqrt(t))
    d2 = d1 - vol * np.sqrt(t)

    if is_call:
        return 0.01 * k * t * np.exp(-r * t) * norm_cdf(d2)
    else:
        return 0.01 * -k * t * np.exp(-r * t) * norm_cdf(-d2)

