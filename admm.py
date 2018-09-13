def ADMM(data,parameter):

    def w_update(data,z,u):
        return w

    def z_update(w,u):
        return z


    MAX_ITER = 10
    for i in range(MAX_ITER):
        w = w_update(data, z, u)
        z = z_update(w, u)
        u = u + w - z
