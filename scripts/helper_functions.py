import numpy as np
import pandas as pd

import mmh3

'''
Helper functions for detector
''' 


def KL_divergence(P, Q):
    """
    Calculates the KL divergence Dkl(P||Q)
    """
    p = P/max(1, P.sum())
    q = Q/max(1, Q.sum())
    div = np.divide(q, p, out=np.zeros_like(p), where=((p!=0)&(q!=0)))
    logdiv = np.log(div, out=np.zeros_like(div), where=(div!=0))
    D = -np.sum(p*logdiv)
    return D


def hash_to_buckets(entries, bucket_limits, seed):
    hashed = np.array([mmh3.hash(str(e), seed, signed=False) for e in entries])
    buckets = np.digitize(hashed, bucket_limits, right=True)
    return buckets


def int_ext_filter(frame):
    """
    Sorts values into internal/external.
    Note that it is not sensitive to internal -> internal
    """
    frame = frame.copy()
    frame['internal'] = ''
    frame['external'] = ''
    cond = (frame.src_addr.str.startswith('172.20') | 
            frame.src_addr.str.startswith('172.21'))

    # internal -> external
    frame.loc[cond, 'internal'] = frame.loc[cond].src_addr
    frame.loc[cond, 'external'] = frame.loc[cond].dst_addr

    cond = (frame.dst_addr.str.startswith('172.20') | 
            frame.dst_addr.str.startswith('172.21'))

    # external -> internal
    frame.loc[cond, 'internal'] = frame.loc[cond].dst_addr
    frame.loc[cond, 'external'] = frame.loc[cond].src_addr

    # Drops rows which cannot be classified by the internal/external rules.
    frame = frame.drop(frame.loc[(frame.internal == '') | (frame.external == '')].index)

    return frame


def protocol_filter(proto):
    def p_filter(frame):
        frame = frame.copy()
        subframe = frame.loc[
                frame.ip_protocol == proto
                ]
        filtered = int_ext_filter(subframe)
        return filtered
    return p_filter


def detection_list_to_df(det_list):
    flat = []
    for sublist in det_list:
        for det in sublist:
            flat.append(det)

    detector = []
    operational = []
    feature = []
    value = []
    number = []
    timestep = []
    for det in flat:
        detector.append(det.detector)
        operational.append(det.operational)
        feature.append(det.feature)
        value.append(det.value)
        number.append(det.number)
        timestep.append(det.timestep)
    return pd.DataFrame(
            {'detector': detector,
             'operational': operational,
             'feature': feature,
             'value': value,
             'number': number,
             'timestep': timestep
            })


def common_ip_filter(df):
    ip_google = [('35.190.247.0', '35.190.247.255'),
                 ('35.191.0.0', '35.191.255.255'),
                 ('64.233.160.0', '64.233.191.255'),
                 ('66.102.0.0', '66.102.15.255'),
                 ('66.249.80.0', '66.249.95.255'),
                 ('72.14.192.0', '72.14.255.255'),
                 ('74.125.0.0', '74.125.255.255'),
                 ('108.177.8.0', '108.177.15.255'),
                 ('108.177.96.0', '108.177.127.255'),
                 ('130.211.0.0', '130.211.3.255'),
                 ('209.85.128.0', '209.85.255.255'),
                 ('216.58.192.0', '216.58.223.255'),
                 ('216.239.32.0', '216.239.63.255')]
