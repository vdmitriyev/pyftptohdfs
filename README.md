### About

Porting files from FTP to HDFS via Python. Must be run on Hadoop cluster with accessible utilities (e.g.) from Hadoop.

### Usage

* Change configurations inside 'pyftptohdfs.py' (e.g. credentials) or use command line to pass them
* Run
```
python pyftptohdfs.py --downloadonly
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
