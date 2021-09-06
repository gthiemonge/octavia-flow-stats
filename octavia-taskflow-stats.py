#!/usr/bin/env python

import collections
import csv
import datetime
import re
import sys

log_re = re.compile(
    "^(?P<date>(\w{3} [0-9 ]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]*)?|[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3})) ")

taskflow_re = re.compile(
    "(?P<type>Task|Flow) '(?P<name>[^']*)' "
    "\((?P<id>[a-f0-9-]*)\) "
    "transitioned into state '(?P<new_state>[A-Z]*)' "
    "from state '(?P<prev_state>[A-Z]*)'")

taskflows = {
    'tasks': {},
    'flows': {},
}

def taskflows_dump(taskflows):
    items = [(s, k, taskflows[s][k]) for s in taskflows for k in taskflows[s]]

    writer = csv.writer(sys.stdout)
    writer.writerow(("type", "name", "state", "started", "updated", "duration"))

    for elem in sorted(items, key=lambda e: e[2]['updated'], reverse=True):
        store, uuid, v = elem
        name = v['name']
        states = v['last_state']

        writer.writerow((
            store, name, states,
            v['started'].strftime("%H:%M:%S"),
            v['updated'].strftime("%H:%M:%S"),
            str(v['duration'])))

for l in sys.stdin:
    m = log_re.match(l)
    if not m:
        continue

    d = m.groupdict()

    try:
        t = datetime.datetime.strptime(d['date'][:15], "%b %d %H:%M:%S")
    except ValueError:
        t = datetime.datetime.strptime(d['date'][:-4], "%Y-%m-%d %H:%M:%S")

    m = taskflow_re.search(l)
    if m:
        d = m.groupdict()

        if d['type'] == 'Task':
            store = 'tasks'
        elif d['type'] == 'Flow':
            store = 'flows'
        else:
            raise Exception(d['type'])

        if d['id'] not in taskflows[store]:
            taskflows[store][d['id']] = {
                'name': d['name'],
                'last_state': d['prev_state'],
                'started': t,
            }
        elem = taskflows[store][d['id']]

        elem['last_state'] = d['new_state']
        elem['duration'] = t - elem['started']
        elem['updated'] = t

taskflows_dump(taskflows)
