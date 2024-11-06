import blackscholes

p = blackscholes.price(100, 95, 0.05, 0.6, 0.4, True, False)
print(f"BlackScholes Price: {p}")
