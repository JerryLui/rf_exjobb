"""
Log analyzer for logs generated from elasticquery.py and detector.py.
Run with log file path as input

ex.
python3 log_analyzer logs/l301209.log

@Author Jerry Liu jerry.liu@recordedfuture.com
"""
from datetime import datetime
import numpy as np


def analyze(read_file):
    print('-'*40)
    print('File:\t%s' % read_file)
    with open(read_file, 'r') as f:
        lines = f.readlines()

    structured_lines = []
    last_time = datetime(1900, 1, 1)
    last_line_time = datetime(1900, 1, 1)

    cycle_times = []
    detection_times = []
    batch_sizes = []
    batch_times = []
    empty_respones = []
    start_time = None
    finished = False
    for i, line in enumerate(lines):
        line = line.rstrip().split()
        try:
            log_time = datetime.strptime(' '.join(line[:2]), '%m/%d/%Y %H:%M:%S,%f')
            log_message = line[4]
            structured_lines.append((log_time, log_message))

            if not start_time:
                start_time = log_time

            if log_message not in ['Processed', 'Querying', 'Processing', 'Finished', 'Entries']:
                detection_times.append((log_time - last_line_time).total_seconds())
            elif log_message == 'Processed':
                cycle_times.append((log_time - last_time).total_seconds())
                batch_times.append((log_time - last_line_time).total_seconds())
                last_time = log_time
                batch_sizes.append(int(line[5]))
            elif log_message == 'Entires':
                empty_respones.append(lines[i-1])
            elif log_message == 'Finished':
                finished = True
            last_line_time = log_time

        except Exception as e:
            continue
    end_time = last_line_time

    cycle_times = cycle_times[1:]
    detection_times = detection_times[1:]
    
    print('Done:\t%s' % finished)
    print('Total:\t%.2f min' % ((end_time - start_time).total_seconds()/60))
    print('\t%i batches' % len(batch_sizes))
    print('\t%i empty' % len(empty_respones))

    print('Batch Sizes')
    print('Mean:\t%.2f' % (np.mean(batch_sizes)))
    print('Median:\t%i' % (np.median(batch_sizes)))

    print('Batch Times')
    print('Mean:\t%.2f' % np.mean([batch_times[i]/batch_sizes[i] for i in range(len(batch_times))]))
    print('Median:\t%.2f' % np.median([batch_times[i]/batch_sizes[i] for i in range(len(batch_times))]))

    print('Cycle Times')
    print('Mean:\t%.2f' % (np.mean(cycle_times)))
    print('Median\t%.2f' % (np.median(cycle_times)))

    print('Detection Times')
    print('Max:\t%.2f' % (np.max(detection_times)))
    print('Median:\t%.2f' % (np.median(detection_times)))

    print('-'*20)
    for empty_respone in empty_respones:
        print(empty_respone)

    return structured_lines


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        read_files = [sys.argv[i] for i in range(1, len(sys.argv))]
    else:
        read_files = ['/home/jerry/Dropbox/Kurser/Master Thesis/rf_exjobb/scripts/logs/l151602.log']

    results = [analyze(file) for file in read_files]
