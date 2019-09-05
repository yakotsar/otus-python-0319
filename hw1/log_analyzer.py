#!/usr/bin/env python3


# log_format ui_short   '$remote_addr $remote_user $http_x_real_ip [$time_local] "$request" '
#                       '$status $body_bytes_sent "$http_referer" '
#                       '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER"'
#                       '$request_time';
import argparse
import os
import json
import csv
import datetime
import gzip
import re
import statistics
import logging


config = {
        "REPORT_SIZE": 1000,
        "REPORT_DIR": "./reports",
        "LOG_DIR": "./log",
        }


def main(config):
    report_name = get_report_name()
    if os.path.exists(os.path.join(config['REPORT_DIR'] , report_name)):
        return
    report_path = os.path.join(config['REPORT_DIR'], report_name)
    log = read_log()
    parsed_log = parse_log(log)
    grouped_data = group_data(parsed_log) 
    report = make_report(grouped_data, config['REPORT_SIZE'])
    save_report(report, report_path)

def get_report_name():
    today = datetime.date.today()
    return 'report-' + datetime.date.strftime(today, '%Y.%m.%d') + '.html'
    
def read_conf():
    if args.config:
        with open(args.config, 'r') as read_file:
            data = json.load(read_file)
            config.update(data)

def read_log():
    recent_log = get_recent_log(os.scandir(config['LOG_DIR']))
    if recent_log:
        log_path = recent_log
        open_func = gzip.open if os.path.splitext(log_path)[-1] == '.gz' else open
        with open_func(log_path, 'r') as read_file:
            for line in read_file:
                yield line

def get_recent_log(scandir):
    pattern = re.compile(r'nginx-access-ui.log-(\d{4})(\d{2})(\d{2})(?:\.\w+)*')
    entries = [entry for entry in scandir if entry.is_file() and pattern.fullmatch(entry.name)]
    if entries:
        recent_entry = max(entries, key = lambda entry: datetime.date(*map(int, *pattern.findall(entry.name))))
        return recent_entry.path

def parse_log(log):
    pattern = re.compile(r'"\S+ (\S+).*" \d+ \d+ ".+" ".+" ".+" ".+" ".+" ([.\d]+)$')
    for line in log:
        match = pattern.search(line)
        if match:
            url, req_time = match.groups()
            yield {'url':url, 'req_time':float(req_time)}

def group_data(parsed_log):
    grouped_data = {}
    for line in parsed_log:
        url = line['url']
        req_time = line['req_time']
        req_time_history = grouped_data.get(url)
        if req_time_history:
            grouped_data[url].append(req_time) 
        else:
            grouped_data[url] = [req_time]
    return grouped_data


def make_report(grouped_data, max_size):
    rows = []
    time_total = 0
    requests_total = 0
    for url in grouped_data:
        req_time_history = grouped_data.get(url)
        requests_count = len(req_time_history)
        time_sum = sum(req_time_history)
        row = {
                'url': url,
                'count': requests_count,
                'time_avg': time_sum / requests_count,
                'time_max': max(req_time_history),
                'time_sum': time_sum,
                'time_med': statistics.median_low(req_time_history),
                'time_perc': None,
                'count_perc': None,
                }
        rows.append(row)
        time_total += time_sum
        requests_total += requests_count
    for row in rows:
        row['time_perc'] = 100.0 * row['time_sum'] / time_total
        row['count_perc'] = 100.0 * row['count'] / requests_total
    rows = sorted(rows, key=lambda r: r['time_sum'], reverse=True)
    head_rows = rows[:max_size]
    round_func = lambda x: round(x, 3) if type(x) is float else x
    for row in head_rows:
        for k,v in row.items():
            row[k] = round_func(v)
    return rows[:max_size]

def render(template, table_json):
    return template.replace('$table_json', table_json)

def save_report(rows, file_path):
    with open('./report.html', 'r') as template_file, open(file_path, 'w') as write_file:
        table_as_string = json.dumps(rows)
        template = template_file.read()
        s = template.replace('$table_json', table_as_string)
        write_file.write(s)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='log analyzer')
    parser.add_argument('--config', metavar='[FILE]', type=str, default = './config',
            help='path to config file')
    args = parser.parse_args()
    
    def read_conf(config_path, config):
        if os.path.exists(config_path) and os.stat(config_path).st_size != 0:
            with open(config_path, 'r') as read_file:
                try:
                    data = json.load(read_file)
                except json.JSONDecodeError:
                    return
                config.update(data)
        else:
            return

    read_conf(args.config, config)

    logging.basicConfig(filename=config.get('LOGFILE'), level = logging.INFO)
    formatter = logging.Formatter('[%(asctime)s] %(levelname).1s %(message)s')
    #logging.setFormatter(formatter)
    date_format = '%Y.%m.%d %H:%M:%S'
    
    main(config)
