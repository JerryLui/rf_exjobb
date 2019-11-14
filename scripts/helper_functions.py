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

def int_ext_filter(frame):
    '''
    Sorts values into internal/external.
    Note that it is not sensitive to internal -> internal
    '''
    cond = (frame.src_addr.str.startswith('172.20') | 
            frame.src_addr.str.startswith('172.21'))

    #internal -> external
    frame.loc[cond, 'internal'] = frame.loc[cond].src_addr
    frame.loc[cond, 'external'] = frame.loc[cond].dst_addr

    cond = (frame.dst_addr.str.startswith('172.20') | 
            frame.dst_addr.str.startswith('172.21'))

    #external -> internal
    frame.loc[cond, 'internal'] = frame.loc[cond].dst_addr
    frame.loc[cond, 'external'] = frame.loc[cond].src_addr

    return frame

def protocol_filter(proto):
    def p_filter(frame):
        subframe = frame.loc[
                frame.ip_protocol == proto
                ] 
        return subframe
    return p_filter


