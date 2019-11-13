import numpy as np
import pandas as pd
from detector import Detector, DetectorPool

def get_dummy_data():
    PATH = '~/Chalmers/Thesis/Query/batches/100_mil_7.csv'
    data = pd.read_csv(PATH, index_col=None, header=0)
    print('Successfully read dummy data')
    data['time'] = pd.to_datetime(data.time)
    data['first_switched'] = pd.to_datetime(data.first_switched)
    data['last_switched'] = pd.to_datetime(data.last_switched)
    return data

if __name__ == '__main__':
    dp = DetectorPool()
    
    det = Detector(
            name='Detector',
            n_seeds=8,
            n_bins=256,
            mav_steps=5,
            features=['src_addr', 'dst_addr'],
            filt=None,
            thresh=0.1
            )

    dp.add_detector(det)

    dummy_data = get_dummy_data()

    print('Seems to work, no?')
