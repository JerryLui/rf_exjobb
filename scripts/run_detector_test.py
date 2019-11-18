import numpy as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt

from helper_functions import int_ext_filter, protocol_filter
from detector import Detector, DetectorPool


def get_dummy_data():
    PATH = '~/Chalmers/Thesis/Query/batches/100_mil_6.csv'
    data = pd.read_csv(PATH, index_col=None, header=0)
    print('Successfully read dummy data')
    data['time'] = pd.to_datetime(data.time)
    data['first_switched'] = pd.to_datetime(data.first_switched)
    data['last_switched'] = pd.to_datetime(data.last_switched)
    return data

if __name__ == '__main__':
    dp = DetectorPool()
    
    int_ext = Detector(
            name='int_ext',
            n_seeds=8,
            n_bins=256,
            mav_steps=3,
            features=['internal', 'external'],
            filt=int_ext_filter,
            thresh=0.05
            )
    dp.add_detector(int_ext)

    dummy = get_dummy_data()
    
    steps = 20

    divs = np.zeros((8, steps))
    mav  = np.zeros((steps, ))

    step_len = 5 #min
    min_time = dummy.time.min()
    for i in range(steps):
        subwin = dummy.loc[
                (dummy.time >= min_time + pd.Timedelta(minutes=i*step_len)) &
                (dummy.time <  min_time + pd.Timedelta(minutes=(i+1)*step_len))
                ]
        print('Running timestep:\t%i' % i)
        results = dp.run_next_timestep(subwin)
        print(results[0])
        #print('Detection frame contains %i rows' % results[1].shape[0])
        print(results[1].head(10))

        #new_div = dp.get_detector_divs()['icmp']
        #divs[:, i] = new_div[0, :]
        #new_mav = dp.get_detector_mavs()['icmp']
        #mav[i] = new_mav[0]

    #fig, ax = plt.subplots()
    #for n in range(8):
    #    ax.plot(divs[n,:])
    #ax.plot(mav, 'r+-')
    #plt.show()

    print('Seems to work, no?')
