import json

FILE_NAME = 'threshold_logs_key.txt'
threshold_key=''
while '__' not in threshold_key:
    threshold_key = raw_input('Enter threshold key in format(countrycode__key):').strip()
threshold_val = raw_input('Enter threshold value: ').strip()
json_data = json.load(open(FILE_NAME, 'r'))
json_data[threshold_key] = threshold_val

with open(FILE_NAME, 'w') as save_threshold:
    save_threshold.write(json.dumps(json_data, sort_keys=True, indent=4))
