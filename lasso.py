import numpy as np
from numpy import linalg as la
from scipy import sparse
import matplotlib.pyplot as plt

# problem: minimize (1/2)||Ax-b||_2^2+lambda*||x||_1

MAX_ITER = 100    # Maximum number of update iterations
ABS_TOL = 1e-4  # absolute tolerance
REL_TOL = 1e-4  # relative tolerance


# soft thresholding operator S(Page 32)
def shrinkage(a, k):
    z = np.maximum(0, a - k) - np.maximum(0, -a - k)
    return z


def objfunction(A, b, lamda, x, z):
    a = np.dot(A, x) - b
    p = 1 / 2 * np.sum(np.power(a, 2)) + lamda * la.norm(z, 1)
    return p


def lassoSolver(A, b, lamda, rho):
    # parameter
    # rho: Penalty parameter

    m, n = A.shape
    print(n)

    x = np.zeros((n, 1))
    z = np.zeros((n, 1))
    u = np.zeros((n, 1))

    primary_residual_list = list()
    dual_residual_list = list()
    obj_val_list = list()

    print(objfunction(A, b, lamda, x, z))

    # update process (Page 43)
    for i in range(0, MAX_ITER):
        print(i)
        # x update
        q = A.T * b + rho * (z - u)
        p = np.transpose(np.dot(A.T, A) + rho * np.eye(n))
        x = np.dot(la.inv(p), q)

        # z update
        z_k = np.copy(z)
        z = shrinkage(x + u, lamda / rho)

        # u update
        u = u + x - z

        primary_residual = la.norm(x - z)
        dual_residual = la.norm(-rho * (z - z_k))

        primary_residual_list.append(primary_residual)
        dual_residual_list.append(dual_residual)

        obj_val_list.append(objfunction(A, b, lamda, x, z))
        print(objfunction(A, b, lamda, x, z))

        # absolute and relative criterion(Page 19)
        eps_primary = np.sqrt(n) * ABS_TOL + REL_TOL * np.maximum(la.norm(x), la.norm(-z))
        eps_dual = np.sqrt(n) * ABS_TOL + REL_TOL * la.norm(rho * u)

        if primary_residual < eps_primary and dual_residual < eps_dual:
            break

    return x, obj_val_list


if __name__ == '__main__':
    m = 1500  # number of examples
    n = 5000  # number of features
    p = np.true_divide(100, n)  # sparsity density

    # 稀疏正态分布随机矩阵
    x0 = np.random.randn(int(n * p), 1)
    z = np.zeros([n - len(x0), 1])
    x0 = np.vstack((x0, z))
    np.random.shuffle(x0)
    x0 = np.mat(x0)

    # 二维正态分布
    A = np.random.randn(m, n)
    A = np.mat(A)

    A = np.dot(A, sparse.spdiags(1 / np.sqrt(sum(np.multiply(A, A))), 0, n, n).todense())  # normalize column
    b = np.dot(A, x0) + np.sqrt(0.001) * np.random.randn(m, 1)

    lambda_max = la.norm(np.dot(A.T, b), np.inf)
    lamda = 0.1 * lambda_max

    # Solve problem
    x, objVal = lassoSolver(A, b, lamda, 1.0)

    plt.plot(np.arange(len(objVal)), objVal)
    plt.xlabel('iter (k)')
    plt.ylabel('f(x^k) + g(z^k)')
    plt.show()


