from datetime import datetime
import numpy as np

read_file = 'rf_exjobb/scripts/l141242.log'
with open(read_file, 'r') as f:
    lines = f.readlines()

structured_lines = []
last_time = datetime(1900, 1, 1)
avg_time = []
for line in lines:
    line = line.rstrip().split()
    try:
        log_time = datetime.strptime(line[1], '%H:%M:%S,%f')
        log_message = line[4]
        structured_lines.append((log_time, log_message))

        if log_message == 'Processed':
            avg_time.append((log_time - last_time).seconds)
            last_time = log_time

    except ValueError:
        continue

avg_time = avg_time[1:]
print(np.mean(avg_time[-12:]))
print(np.median(avg_time[-12:]))


