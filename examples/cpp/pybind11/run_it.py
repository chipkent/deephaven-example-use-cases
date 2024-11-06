# import example
import blackscholes

# result = example.add(1.0, 2.0)
# print(result)  # Output: 3.0

p = blackscholes.price(100, 95, 0.05, 0.6, 0.4, True, False)
print(f"BlackScholes Price: {p}")
