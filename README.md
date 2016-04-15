### About

Transporting files from FTP to HDFS via Python (single threaded, via local temporal folder). Must be run on Hadoop cluster or on PC with accessible Hadoop utilities.

### Usage

* Change configurations inside 'pyftptohdfs.py' (e.g. credentials) or use command line to pass them (--help for details)
* Run in "download only" mode or clearing local folder after finishing
```
python pyftptohdfs.py --downloadonly
python pyftptohdfs.py --clearlocal
```

* Possible measurement of performance manually
```
python -m cProfile -o pyaxtTimeStats.profile pyftptohdfs.py
python -m pstats pyaxtTimeStats.profile copy_from_ftp
stats copy_from_ftp
stats merge_files
stats copy_to_hdfs
```

### Dependencies

* Windows
    + 'sh' must be installed on machine
