#include <cmath>
#include <vector>

#include <pybind11/pybind11.h>

double norm_cdf(double x) {
    return (1.0 + std::erf(x / std::sqrt(2.0))) / 2.0;
}

double norm_pdf(double x) {
    return std::exp(-x * x / 2.0) / std::sqrt(2.0 * M_PI);
}

double price(double s, double k, double r, double t, double vol, bool is_call, bool is_stock) {
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

double delta(double s, double k, double r, double t, double vol, bool is_call, bool is_stock) {
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

// The name "gamma" conflicts on some systems, so using a unique name
double bs_gamma(double s, double k, double r, double t, double vol, bool is_stock) {
    if (is_stock) {
        return 0.0;
    }

    double d1 = (std::log(s / k) + (r + vol * vol / 2.0) * t) / (vol * std::sqrt(t));

    return norm_pdf(d1) / (s * vol * std::sqrt(t));
}

double theta(double s, double k, double r, double t, double vol, bool is_call, bool is_stock) {
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

double vega(double s, double k, double r, double t, double vol, bool is_stock) {
    if (is_stock) {
        return 0.0;
    }

    double d1 = (std::log(s / k) + (r + vol * vol / 2.0) * t) / (vol * std::sqrt(t));
    return s * std::sqrt(t) * norm_pdf(d1);
}

double rho(double s, double k, double r, double t, double vol, bool is_call, bool is_stock) {
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

// This defines the Python module to be created
PYBIND11_MODULE(blackscholes, m) {
    m.def("norm_cdf", &norm_cdf, "norm_cdf");
    m.def("norm_pdf", &norm_pdf, "norm_pdf");
    m.def("price", &price, "price");
    m.def("delta", &delta, "delta");
    m.def("gamma", &bs_gamma, "gamma");
    m.def("theta", &theta, "theta");
    m.def("vega", &vega, "vega");
    m.def("rho", &rho, "rho");
}
