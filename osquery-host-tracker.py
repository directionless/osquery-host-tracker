#!/usr/bin/env python

import sys
import json
import os
import subprocess

# Basic algo
#  1. parse lines
#  2. For each line, extra host, data, timestamp
#  3. Read host entry into cache
#  4. Apply the data diff
#  5. If the minute changed, write to disk and commit

def parse_line(line):
    if 'osqueryd' not in line:
        return(None)
    fields = line.split(' ', 5)
    # timestamp = fields[0:3]
    raw_data = fields[5]

    try:
        data = json.loads(raw_data)
    except ValueError as e:
        # sometimes osquery emits data that isn't json. Just skip it
        return(None)

    return(data)


def load_file(filename):
    try:
        with open(filename, 'r') as file:
            return json.load(file)
    except IOError:
        # missing file
        return []

def commit_to_git(timestamp):
    subprocess.call(['git', 'add', 'data/'])
    subprocess.check_call(['git', 'commit', '-q', '--date', "%s +0000" % timestamp, '-m', 'auto'])

def commit_to_disk(data_cache):
    writes = 0
    for hostid in data_cache:
        directory = "data/%s" % hostid
        if not os.path.exists(directory):
            os.makedirs(directory)
        for queryname in data_cache[hostid]:
            with open("data/%s/%s.json" % (hostid, queryname), 'w') as file:
                json.dump(data_cache[hostid][queryname], file,
                          indent=2,
                          sort_keys=True,
                          separators=(',', ': '))
                file.write("\n")
                writes += 1
    return writes



def parse_log(filename):
    last_parse_time = 0
    data_cache = {}
    with open(filename,'r') as file:
        for line in file:
            data = parse_line(line)
            if data is None:
                continue

            hostguid = data[u'decorations'][u'host_uuid']
            hostname = data[u'hostIdentifier']
            
            queryname = data[u'name']
        
            if data_cache.get(hostname, None) is None:
                data_cache[hostname] = {}
            
            if data_cache[hostname].get(queryname, None) is None:
                data_cache[hostname][queryname] = load_file("data/%s/%s" % (hostname, queryname))

            # This is gross
            #
            # What's happening here, is that these aren't simple
            # object. They're elements of an array. Thus, we need to
            # treat the whole thing as add/removes on an array.
            columndata = data[u'columns']
            if data[u'action'] == 'added':
                data_cache[hostname][queryname].append(columndata)
            elif data[u'action'] == 'removed':
                try:
                    data_cache[hostname][queryname].remove(columndata)
                except ValueError:
                    # We don't normally want this, but since we're not
                    # starting from the beginning of time, we're going
                    # to have remove events for things that don't
                    # match
                    pass
            else:
                print("Unknown action: %s" % data[u'action'])
    

            # There's some obvious optimization to be had here.
            # This is being called for every time change, even if there are no files written.
            # It would be better not to go through the commit_to_disk() routine if not needed.
            timestamp = int(data[u'unixTime'])
            # Is this in the same block of changes?
            if timestamp - last_parse_time > 120:
                files_written = commit_to_disk(data_cache)
                print("Flushed %s files at %s" % (files_written, timestamp))
                if files_written > 0:
                    commit_to_git(timestamp)
                    # flush data_cache, less to iterate later
                    data_cache = {}
                last_parse_time = timestamp

def main():
    parse_log(sys.argv[1])

main()
