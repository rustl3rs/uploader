# Uploader
Uploader is designed to be a sidecar for kubernetes pods that need to save files to cloud config.  At present, only the S3 uploader has been implemented.

Uploader is writen in Rust.  This was a learning project, but the code quality should be high.

A docker image is available from https://hub.docker.com/u/pms1969/uploader-s3  
This image is based on *scratch*

## How does it work?
This is a command line utility that can be oneshot (init container) or a daemon, for continuously monitoring a directory tree and uploading files to the designated cloud storage.

```bash
$ uploader --help
uploader

USAGE:
    uploader [FLAGS] <directory> --map <KEY=VALUE>...

FLAGS:
    -d, --daemon     Maps a subdirectory to an S3 bucket.
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -m, --map <KEY=VALUE>...    Maps a subdirectory to an S3 bucket.
                                subdirectory=s3_bucket_name_with_prefix.
                                e.g. blah=my_bucket/prefix/goes/here
                                Must set a default for the directory `.`.
                                e.g. .=my_bucket/default

ARGS:
    <directory>    Sets the directory to be watched.
```

Subdirectories of the one monitored can be made to upload to different destinations.  A default destination for the monitored directory must always
be given.  Unless a single destination is desired, I would recommend only ever writing to subdirectories, and having mappings for each.

**On upload to cloud storage, the files are deleted from the watched directory.**

---
### **Example: Single monitored directory**
```bash
$ uploader -d /tmp/uploader -m .=dbucket
```

This will start a daemon that watches the `/tmp/uploader` directory, and loads any file writen to any subdirectory to `s3://dbucket/prefix/filename`.  

The following table shows origin directory, and destination bucket/prefix.

| path to file | destination |
| --- | --- |
| `/tmp/uploader/testfile1.txt` | `s3://dbucket/testfile1.txt` |
| `/tmp/uploader/testfile3.txt` | `s3://dbucket/testfile3.txt` |
| `/tmp/uploader/subdir1/anothersubdir/testfile2.txt` | `s3://dbucket/subdir1/anothersubdir/testfile2.txt` |

and so on.

### **Example: Multiple monitored directories**
```bash
$ uploader -d /tmp/uploader -m .=dbucket/default -m subdir1=dbucket/Important -m subdir2=another_bucket/NeedsPriority/app1
```

Again, this starts a daemon, and monitors the `/tmp/uploader` directory.  Any file that is not writen to `/tmp/uploader/subdir1` or `/tmp/uploader/subdir2` is uploaded to `s3://dbucket/prefix/filename`

The following table shows origin directory, and destination bucket/prefix.

| path to file | destination |
| --- | --- |
| `/tmp/uploader/testfile1.txt` | `s3://dbucket/testfile1.txt` |
| `/tmp/uploader/testfile3.txt` | `s3://dbucket/testfile3.txt` |
| `/tmp/uploader/subdir1/testfile2.txt` | `s3://dbucket/Important/testfile2.txt` |
| `/tmp/uploader/subdir1/app2/testfile4.txt` | `s3://dbucket/Important/app2/testfile4.txt` |
| `/tmp/uploader/subdir1/app3/testfile5.txt` | `s3://dbucket/Important/app3/testfile5.txt` |
| `/tmp/uploader/subdir1/app3/trace/testfile6.txt` | `s3://dbucket/Important/app3/trace/testfile6.txt` |
| `/tmp/uploader/subdir2/testfile7.txt` | `s3://another_bucket/NeedsPriority/app1/testfile7.txt` |
| `/tmp/uploader/subdir2/module2/testfile8.txt` | `s3://another_bucket/NeedsPriority/app1/module2/testfile8.txt` |

and so on.

---
## Roadmap

[ ] Integration tests  
[ ] Memory and CPU benchmarking  
[ ] Azure Blob Storage support  
[ ] Split docker images for S3 and Azure Blob Storage  
[ ] Ensure later uploads of failed files, once a successful upload has occurred  

Extend configuration

> [ ] Retries  
> [ ] Initial duration  
> [ ] Max delay for retry  
> [ ] Web port  
> [ ] Web bind address  
> [ ] Grace period for shutdown  
> [ ] Shutdown quiet period  

---
## Contributing

You can help the project in any number of ways.  

Raise **Issues** if you find anything quirky about the app. Please be as descriptive as possible. Explaining the problem as clearly and thoroughly as possible will allow for the highest quality responses.

**Pull Requests** are always welcome.  Please provide as much context for the PR as you can in the description.
