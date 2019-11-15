from datetime import datetime
import numpy as np


def analyze(read_file):
    print('\nFile:\t%s' % read_file)
    with open(read_file, 'r') as f:
        lines = f.readlines()

    structured_lines = []
    last_time = datetime(1900, 1, 1)
    last_line_time = datetime(1900, 1, 1)

    cycle_times = []
    detection_times = []
    for line in lines:
        line = line.rstrip().split()
        try:
            log_time = datetime.strptime(line[1], '%H:%M:%S,%f')
            log_message = line[4]
            structured_lines.append((log_time, log_message))

            if log_message not in ['Processed', 'Querying', 'Processing']:
                detection_times.append((log_time - last_line_time).total_seconds())
            elif log_message == 'Processed':
                cycle_times.append((log_time - last_time).total_seconds())
                last_time = log_time
            last_line_time = log_time

        except Exception as e:
            continue

    cycle_times = cycle_times[1:]
    detection_times = detection_times[1:]

    print('Cycle Times')
    print('Mean:\t%.2f' % (np.mean(cycle_times)))
    print('Median\t%.2f' % (np.median(cycle_times)))

    print('Detection Times')
    print('Mean:\t%.2f' % (np.mean(detection_times)))
    print('Median:\t%.2f' % (np.median(detection_times)))


    return structured_lines


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        read_files = [sys.argv[i] for i in range(1, len(sys.argv))]
    else:
        read_files = ['rf_exjobb/scripts/l151202.log']

    results = [analyze(file) for file in read_files]
