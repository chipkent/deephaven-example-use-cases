#ifndef OPTION_MODEL_H
#define OPTION_MODEL_H

double norm_cdf(double x);
double norm_pdf(double x);

double price(double s, double k, double r, double t, double vol, bool is_call, bool is_stock);
double delta(double s, double k, double r, double t, double vol, bool is_call, bool is_stock);
double gamma(double s, double k, double r, double t, double vol, bool is_stock);
double theta(double s, double k, double r, double t, double vol, bool is_call, bool is_stock);
double vega(double s, double k, double r, double t, double vol, bool is_stock);
double rho(double s, double k, double r, double t, double vol, bool is_call, bool is_stock);

#endif // OPTION_MODEL_H