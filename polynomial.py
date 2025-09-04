import numpy as np
import scipy.optimize

# Given data points
voltage = np.array([1.00, 1.93, 2.87, 3.70, 4.41, 5.00])
flow_rate = np.array([0, 4, 8, 12, 16, 20])

# Fit a quadratic function: y = ax^2 + bx + c
quadratic_coeffs = np.polyfit(voltage, flow_rate, 2)

# Fit a cubic function: y = ax^3 + bx^2 + cx + d
cubic_coeffs = np.polyfit(voltage, flow_rate, 3)

# Return the coefficients for both fits
print("Quadratic:", quadratic_coeffs, "Cubic:", cubic_coeffs)
