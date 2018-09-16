import numpy as np
from numpy import linalg as la
import scipy
from scipy import sparse
import matplotlib.pyplot as plt

# parameter
# rho: Penalty parameter

MAX_ITER = 10
MAX_INNER_ITER = 10
ABS_TOL = 1e-4  # absolute tolerance
REL_TOL = 1e-2  # relative tolerance


# ADMM for consensus problem

def ADMM(data,rho):

    # Varying Penalty Parameter（Page 20)
    def rho_adjustment(rho, norm_pri, norm_dual):
        """
        Parameters
        ----------
        rho : float
            Current RHO value
        norm_pri : float
            Current normed primal residual
        norm_dual : float
            Current normed dual residual
        Returns
        -------
        rho : float
            Updated RHO
        """
        if norm_pri > 10 * norm_dual:
            rho *= 2
        elif norm_dual > 10 * norm_pri:
            rho /= 2

        return rho

    def w_update(data,z,u):

        D = dimension

        p = data.map()

        for j in range(MAX_INNER_ITER):
            w = z

            data.map()

        return w

    def z_update(w,u):

        D = dimension

        z = np.zeros(D)

        for i in range(dimension):
            z[i] = w[i]

        return z

    primary_residual_list = list()
    dual_residual_list = list()

    w = np.zeros((n, 1))
    z = np.zeros((n, 1))
    u = np.zeros((n, 1))

    for i in range(MAX_ITER):
        # update
        w = w_update(data, z, u)
        z_k = np.copy(z)
        z = z_update(w, u)
        u = u + w - z

        primary_residual = la.norm(w - z)
        dual_residual = la.norm(-rho * (z - z_k))

        primary_residual_list.append(primary_residual)
        dual_residual_list.append(dual_residual)

        rho_adjustment(rho, primary_residual, dual_residual)

        # absolute and relative criterion(Page 19)
        # eps_primary = np.sqrt(n) * ABS_TOL + REL_TOL * np.maximum(la.norm(x), la.norm(-z))
        # eps_dual = np.sqrt(n) * ABS_TOL + REL_TOL * la.norm(rho * u)

        if primary_residual < eps_primary and dual_residual < eps_dual:
            break

    return z




