import numpy as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt

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
    
    det = Detector(
            name='det1',
            n_seeds=8,
            n_bins=256,
            mav_steps=5,
            features=['src_addr', 'dst_addr'],
            filt=None,
            thresh=0.05
            )

    dp.add_detector(det)

    dummy = get_dummy_data()
    
    steps = 30

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
        dp.next_timestep(subwin)

        new_div = dp.get_detector_divs()['det1']
        divs[:, i] = new_div[0, :]
        new_mav = dp.get_detector_mavs()['det1']
        mav[i] = new_mav[0]

    fig, ax = plt.subplots()
    for n in range(8):
        ax.plot(divs[n,:])
    ax.plot(mav, 'r+-')
    plt.show()

    print('Seems to work, no?')
