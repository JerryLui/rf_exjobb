import numpy as np
import pandas as pd

import mmh3

'''
Helper functions for detector
''' 

def KL_divergence(P, Q):
    '''
    Calculates the KL divergence Dkl(P||Q)
    '''
    p = P/P.sum()
    q = Q/Q.sum()
    div = np.divide(q, p, out=np.zeros_like(p), where=(p!=0))
    logdiv = np.log(div, out=np.zeros_like(div), where=(div!=0))
    #D = -np.sum(p * np.log(div))
    D = -np.sum(p*logdiv)
    return D

def hash_to_buckets(entries, bucket_limits, seed):
    hashed = np.array([mmh3.hash(str(e), seed, signed=False) for e in entries])
    buckets = np.digitize(hashed, bucket_limits, right=True)
    return buckets
