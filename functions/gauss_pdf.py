import math
import numpy as np
from scipy import stats


def gauss_pdf(x, mu=0, sigma=1):
    return (np.exp(-((x - mu) ** 2 / (2 * sigma ** 2)))) / (np.sqrt(2 * math.pi) * sigma)
