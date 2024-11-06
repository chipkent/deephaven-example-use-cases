#ifndef OPTION_MODEL_H
#define OPTION_MODEL_H

double norm_cdf(double x);
double norm_pdf(double x);

double black_scholes_price(double s, double k, double r, double t, double vol, bool is_call, bool is_stock);
double black_scholes_delta(double s, double k, double r, double t, double vol, bool is_call, bool is_stock);
double black_scholes_gamma(double s, double k, double r, double t, double vol, bool is_stock);
double black_scholes_theta(double s, double k, double r, double t, double vol, bool is_call, bool is_stock);
double black_scholes_vega(double s, double k, double r, double t, double vol, bool is_stock);
double black_scholes_rho(double s, double k, double r, double t, double vol, bool is_call, bool is_stock);

#endif // OPTION_MODEL_H