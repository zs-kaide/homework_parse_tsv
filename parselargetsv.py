#!/usr/bin/env python
# coding:utf-8

import signal
import sys
import os
import glob
import logging
import logging.handlers
import stat
import codecs
import csv
import shutil
import tempfile
import datetime
import click
import pickle
import struct


class ParseRowsTsv(object):

    def __init__(
        self, inputf, outputf
            ):
        self.inputf = inputf
        self.outputf = outputf

    def read_tsv(self):
        rf = codecs.open(self.inputf, encoding='utf-8')
        try:
            reader = csv.reader(rf, delimiter="\t", lineterminator='\n')
            yield map(lambda x: x.decode('utf-8'), reader.next())
            for row in reader:
                row = map(lambda x: x.decode('utf-8'), row)
                row = (
                    int(row[0]),
                    int(row[1]),
                    int(row[2]),
                    float(row[3]),
                    int(row[4]),
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                )
                yield row
            rf.close()
        except Exception as e1:
            rf.close
            raise e1

    def pickle_tsv(self):
        # import pdb; pdb.set_trace()
        try:
            fpath = self.outputf
            fd, temp = tempfile.mkstemp()
            os.close(fd)
            os.chmod(temp, stat.S_IRWXU | stat.S_IROTH)
            fd = codecs.open(temp, "wb", "utf-8")
            for record in self.read_tsv():
                pickle.dump(record, fd)
            shutil.move(temp, fpath)
            fd.close()
        except Exception as e2:
            if os.path.exists(temp):
                os.remove(temp)
            fd.close()
            raise e2

    def struct_tsv(self):
        try:
            fpath = self.outputf
            fd, temp = tempfile.mkstemp()
            os.close(fd)
            os.chmod(temp, stat.S_IRWXU | stat.S_IROTH)
            fd = codecs.open(temp, "wb", "utf-8")
            for record in self.readtsv():
                s = struct.Struct(
                    'i h l d ? %ds %ds %ds %ds' % (
                        len(record[5]), len(record[6]),
                        len(record[7]), len(record[8]),
                        )
                    )
                fd.write(s.pack(*tuple(record)))
            shutil.move(temp, fpath)
            fd.close()
        except Exception as e2:
            if os.path.exists(temp):
                os.remove(temp)
            fd.close()
            raise e2


class SignalException(Exception):
    def __init__(self, message):
        super(SignalException, self).__init__(message)


def do_exit(sig, stack):
    raise SignalException("Exiting")


@click.command()
@click.option(
    '--file', type=click.Choice(['pickle', 'struct']),
    default='pickle')
@click.option('-i', '--inputf', default=r'/tmp/kadai_1.tsv')
@click.option('-o', '--outputf', default=r'/tmp/kadai_2.p')
def cmd(file, inputf, outputf):
    # シグナル
    signal.signal(signal.SIGINT, do_exit)
    signal.signal(signal.SIGHUP, do_exit)
    signal.signal(signal.SIGTERM, do_exit)
    # ログハンドラーを設定する
    LOG_MANYROWSTSV = 'logging_warning.out'
    my_logger = logging.getLogger('MyLogger')
    my_logger.setLevel(logging.WARNING)
    handler = logging.handlers.RotatingFileHandler(
        LOG_MANYROWSTSV, maxBytes=2000, backupCount=5,)
    my_logger.addHandler(handler)

    parser = ParseRowsTsv(inputf, outputf)
    try:
        if file == 'pickle':
            parser.pickle_tsv()
        elif file == 'struct':
            parser.struct_tsv()

    except SignalException as e1:
        my_logger.warning('%s: %s' % (e1, datetime.datetime.now()))
        logfiles = glob.glob('%s*' % LOG_MANYROWSTSV)
        print logfiles
        sys.exit(1)


def main():
    s = datetime.datetime.now()
    print s + datetime.timedelta(0, 0, 0, 0, 0, 9)
    cmd()
    e = datetime.datetime.now()
    print str(e-s)


if __name__ == '__main__':
    main()
