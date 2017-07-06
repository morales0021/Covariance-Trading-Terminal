import numpy as np
import math

def qrm_cov_pc(ret, use_cor = True, excl_first = False):
    print "Running cov matrix function"
    tr = ret.std(axis = 1)

    if(use_cor):
        ret = ret.divide(tr, axis = 'rows')

    d = len(ret.columns)
    x = ret.transpose()
    x = x.cov()
    tv = np.diag(x) #Returns array of diagonal elements
    x = np.linalg.eig(x) #Return array of eigenvalues and eigenvectors


    #Ordering eigenvalues and eigenvectors in decreasing order
    order_eig=x[0].argsort()[::-1]
    x[0][:] = x[0][order_eig]
    x[1][:] = x[1][:,order_eig]

    if(excl_first):
        k1 = 1 #Different than the original version k1<-2
        vec =  x[1].transpose() #auxiliar operation ()
        y1 = math.sqrt(x[0][0]) *  np.resize(vec[0],(1,len(ret.axes[0]))).transpose()
        x1 = np.dot(y1,y1.transpose())
        tv = tv - np.diag(x1)
    else:
        k1 = 0 #Different than the original version k1<-1
        x1 = 0
        
    g_prev = 999

    for k in range(k1,d-1):
        u = x[0][k1:(k+1)]
        v = x[1][:, k1:(k+1)]
        v = (np.sqrt(u) * v)
        xf = np.dot(v,v.transpose())
        xs = tv - np.diag(xf)
        z = xs / tv

        try:
            g = abs(math.sqrt(min(z)) + math.sqrt(max(z)) - 1)
        except:
            print "breaking"
            break

        if g>g_prev:
            break

        g_prev = g

        spec_risk = np.sqrt(xs)

        if excl_first:
            fac_load = np.concatenate((y1,v), axis = 1)
        else:
            fac_load = v

        fac_cov = np.zeros(shape=(k + 1, k + 1))
        np.fill_diagonal(fac_cov, 1.0) #inplace manipulation

        cov_mat = np.diag(xs) + xf + x1
        
    y_s = 1 / np.square(spec_risk)
    v = fac_load
    v1 = np.multiply(v.transpose(),y_s).transpose()
        
    n_cols = v.shape[1]
    aux_diag = np.zeros((n_cols,n_cols),float)
    np.fill_diagonal(aux_diag, 1.0)
    
    aux_op = np.linalg.inv(aux_diag + np.dot(v.transpose(),v1))
    aux_op = np.dot(aux_op, v1.transpose())
    aux_op = np.dot(v1, aux_op)

    diag_ys = np.zeros((len(y_s), len(y_s)),float)
    np.fill_diagonal(diag_ys, y_s)

    inv_cov = diag_ys - aux_op
        
    if(use_cor):
        spec_risk = tr * spec_risk
        tr = np.array(tr)
        fac_load = (fac_load.T * tr).T
        cov_mat = ( ( (cov_mat.T * tr).T ) * tr ).T
        inv_cov = ( inv_cov / tr ).T  / tr
            
    result = dict()
    result['spec_risk'] = spec_risk
    result['fac_load'] = fac_load
    result['fac_cov'] = fac_cov
    result['cov_mat'] = cov_mat
    result['inv_cov'] = inv_cov
    result['pc'] = x[1][:,0:fac_load.shape[1]]
        
    return result