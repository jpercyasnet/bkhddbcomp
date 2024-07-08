# bkhddbcomp
Pure Rust program: Read the md5sum of a file in csv of HD database and see if it is in the Backup Database.

example:

bkhddbcomp01 bk20240531061717.db3 hdinit.csv exclude.excfile nnnn

   where nnnn is an optional input to read the hdinit.csv starting at nnnn row

bk20240531061717.db3 is backup database 

hdinit.csv is a dump of the hd database

exclude.excfile is a text file which excludes files and directories.

see documentation repository for additional information
