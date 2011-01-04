#!/usr/bin/python

import os
import sys
from boto.s3.connection import S3Connection
from boto.exception import S3ResponseError

VERBOSE = False
MISSED_FILES = '/tmp/s3clone_missed_files'
LOG = '/tmp/s3clone_log'

def usage():
    print "Before running this script you must export AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables"
    print "Usage: %s <source_bucket> <target_bucket>" % sys.argv[0]
    sys.exit(1)

def summary(items_to_clone, cloned_items, error_items):
    print "Total Items to Clone: %d" % items_to_clone
    print "Total Cloned Items:   %d" % cloned_items 
    print "Total Error Items:    %d" % error_items

def fetch_element_attributes(element):
    return {element.name: element.etag}

def compare_element(name, etag, compare_list):
    if name not in compare_list or compare_list[name] != etag:
        return name
    else:
        return None

def log(msg, error = False):
    logfile.write("%s\n" % msg)
    if VERBOSE or error:
        print msg

if 'AWS_ACCESS_KEY_ID' not in os.environ or 'AWS_SECRET_ACCESS_KEY' not in os.environ:
    print "Error: Missing AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY"
    usage()

if len(sys.argv) < 3:
    print "Error: wrong arguments"
    usage()

missed_files = open(MISSED_FILES, 'w')
logfile = open(LOG, 'w')

source_bucket_name = sys.argv[1]
target_bucket_name = sys.argv[2]

conn = S3Connection()

try:
    source_bucket = conn.get_bucket(source_bucket_name)
except S3ResponseError as e:
    if e.status == 404:
        log("Bucket %s doesn't exist" % source_bucket_name, error=True)
    elif e.status == 403:
        log("Not authorized or wrong ACCESS_KEY or SECRET_ACCESS_KEY", error=True)
    sys.exit(1)
except Exception:
    log("Error connecting to source bucket", error=True)
    sys.exit(1)

try:
    target_bucket = conn.create_bucket(target_bucket_name)
except S3ResponseError as e:
    if e.status == 403:
        log("Not authorized or wrong ACCESS_KEY or SECRET_ACCESS_KEY", error=True)
    sys.exit(1)
except Exception:
    log("Error creating Bucket", error=True)
    sys.exit(1)

log("Fetching elements in bucket %s " % source_bucket_name)
src_dic = {}
for element in source_bucket.list():
    src_dic.update(fetch_element_attributes(element))

log("Fetching elements in bucket %s " % target_bucket_name)
dst_dic = {}
for element in target_bucket.list():
    dst_dic.update(fetch_element_attributes(element))

log("Comparing elements from buckets %s and %s" % (source_bucket_name, target_bucket_name))
names_queue = [name for name,etag in src_dic.items() if compare_element(name,etag,dst_dic)]

cloned_items = 0
failed_items = 0

if len(names_queue) > 0:
    log("Copying elements from %s to %s" % (source_bucket_name, target_bucket_name))

for key in names_queue:
    try:
        log("Cloning %s" % key)
        dst_key = target_bucket.copy_key(key, source_bucket_name, key)
        dst_key.set_acl(source_bucket.get_acl(key_name=key))
        cloned_items = cloned_items + 1
        log("Cloned %s" % key)
    except NameError:
        failed_items = failed_items + 1
        missed_files.write("%s\n" % key)
        log("Failed cloning %s" % key, error=True)
       
missed_files.close()
logfile.close()

summary(len(names_queue), cloned_items, failed_items)
