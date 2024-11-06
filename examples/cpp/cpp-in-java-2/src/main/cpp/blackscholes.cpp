#include <cmath>
#include <vector>

double norm_cdf(double x) {
    return (1.0 + std::erf(x / std::sqrt(2.0))) / 2.0;
}

double norm_pdf(double x) {
    return std::exp(-x * x / 2.0) / std::sqrt(2.0 * M_PI);
}

double black_scholes_price(double s, double k, double r, double t, double vol, bool is_call, bool is_stock) {
    if (is_stock) {
        return s;
    }

    double d1 = (std::log(s / k) + (r + vol * vol / 2.0) * t) / (vol * std::sqrt(t));
    double d2 = d1 - vol * std::sqrt(t);

    if (is_call) {
        return s * norm_cdf(d1) - k * std::exp(-r * t) * norm_cdf(d2);
    } else {
        return k * std::exp(-r * t) * norm_cdf(-d2) - s * norm_cdf(-d1);
    }
}

double black_scholes_delta(double s, double k, double r, double t, double vol, bool is_call, bool is_stock) {
    if (is_stock) {
        return 1.0;
    }

    double d1 = (std::log(s / k) + (r + vol * vol / 2.0) * t) / (vol * std::sqrt(t));

    if (is_call) {
        return norm_cdf(d1);
    } else {
        return -norm_cdf(-d1);
    }
}

double black_scholes_gamma(double s, double k, double r, double t, double vol, bool is_stock) {
    if (is_stock) {
        return 0.0;
    }

    double d1 = (std::log(s / k) + (r + vol * vol / 2.0) * t) / (vol * std::sqrt(t));

    return norm_pdf(d1) / (s * vol * std::sqrt(t));
}

double black_scholes_theta(double s, double k, double r, double t, double vol, bool is_call, bool is_stock) {
    if (is_stock) {
        return 0.0;
    }

    double d1 = (std::log(s / k) + (r + vol * vol / 2.0) * t) / (vol * std::sqrt(t));
    double d2 = d1 - vol * std::sqrt(t);

    if (is_call) {
        return -((s * norm_pdf(d1) * vol) / (2.0 * std::sqrt(t))) - r * k * std::exp(-r * t) * norm_cdf(d2);
    } else {
        return -((s * norm_pdf(d1) * vol) / (2.0 * std::sqrt(t))) + r * k * std::exp(-r * t) * norm_cdf(-d2);
    }
}

double black_scholes_vega(double s, double k, double r, double t, double vol, bool is_stock) {
    if (is_stock) {
        return 0.0;
    }

    double d1 = (std::log(s / k) + (r + vol * vol / 2.0) * t) / (vol * std::sqrt(t));
    return s * std::sqrt(t) * norm_pdf(d1);
}

double black_scholes_rho(double s, double k, double r, double t, double vol, bool is_call, bool is_stock) {
    if (is_stock) {
        return 0.0;
    }

    double d1 = (std::log(s / k) + (r + vol * vol / 2.0) * t) / (vol * std::sqrt(t));
    double d2 = d1 - vol * std::sqrt(t);

    if (is_call) {
        return 0.01 * k * t * std::exp(-r * t) * norm_cdf(d2);
    } else {
        return 0.01 * -k * t * std::exp(-r * t) * norm_cdf(-d2);
    }
}
