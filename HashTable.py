import re
import json
import os
import sys

MAX_LOG = 100

class HashTable():
    def __init__(self, logfile, tablefile):
        self.table = {}
        self.logfile = logfile
        self.tablefile = tablefile
        self.log = open(logfile, 'a+')
        self.log_count = 0
        self.load_table()

    def load_table(self):
        try:
            f = open(self.tablefile, 'r')
            self.table = json.load(f)
            f.close()
        except FileNotFoundError:
            self.table = {}

        log_read = open(self.logfile, 'r')
        lines = log_read.readlines()
        ckpt_line = "FOO"
        if len(lines):
            ckpt_line = lines[-1]
        # if the log has CKT, then the server failed during a log compaction
        # we now need to recover the data from the backup checkpoint file if it
        # exists, or just read table.ckpt because its up to date.
        if ckpt_line[:3] == "CKT":
            try:
                os.rename("table1.ckpt", self.tablefile)
            except FileNotFoundError:
                pass
            try:
                with open(self.tablefile) as f:
                    self.table = json.load(f)
            except FileNotFoundError:
                self.table = {}
            # now delete this log file
            self.log.truncate(0)
            self.log_count = 0
        else:
            for line in lines:
                self.log_count += 1
                line = line.strip()
                method = line[:3]

                if method == 'INS':
                    arg = json.loads(line[4:])
                    key = arg["key"]
                    value = arg["value"]
                    self.table[key] = value
                elif method == 'REM':
                    key = json.loads(line[4:])
                    del self.table[key]
                
        log_read.close()

    def dump_table(self, outfile):
        f = open(outfile, 'w')
        f.write(json.dumps(self.table))
        f.flush()
        os.fsync(f)
        f.close()

    # outputs the table to table1.ckpt
    # writes to log that new log file has been created (CKT)
    # then renames ckpt file
    # finally, deletes log file and creates new one 
    def compact_log(self):
        self.log_count = 0
        self.dump_table("table1.ckpt")
        self.log.write("CKT\n")
        self.log.flush()
        os.fsync(self.log)
        os.rename("table1.ckpt", "table.ckpt")
        self.log.truncate(0)


    # output INS {"key": key, "value":  value} as json
    def insert(self, key, value):
        self.log.write("INS ")
        log_output = json.dumps({"key": key, "value": value})
        self.log.write(log_output + "\n")
        self.log.flush()
        os.fsync(self.log)
        self.table[key] = value
        self.log_count+=1
        if self.log_count >= MAX_LOG:
            self.compact_log()

    def lookup(self, key):
        try:
            return self.table[key]
        except KeyError:
            return None

    # output REM {key} as json
    def remove(self, key):
        try:
            if not key in self.table:
                raise KeyError()
            self.log.write("REM ")
            log_output = json.dumps(key)
            self.log.write(log_output + "\n")
            self.log.flush()
            os.fsync(self.log)
            value = self.table[key]
            del self.table[key]
            self.log_count+=1
            if self.log_count >= MAX_LOG:
                self.compact_log()
            return value
        except KeyError:
            return None

    # make sure there are no weird exceptions here
    def scan(self, regex):
        try:
            pattern = re.compile(regex)
            matches = list()
            for key, value in self.table.items():
                if pattern.search(key):
                    matches.append((key, value))
            return matches
        except re.error:
            return "Re.error"
        except:
            return list()

            
