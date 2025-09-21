# Usage
## Make the script executable
``` bash
chmod +x runners.sh
```
## Run Manually
``` bash
./runners.sh
```

## Add to crontab
``` bash
sudo crontab -e
*/30 * * * * LOG_DIR=/tmp/wr/databasename LOCK_DIR=/var/lock/wr/databasename   DROP_AFTER_MERGE=1 PURGE_STAGING_OBJECTS=0 USE_DEBUG=0 KEEP_LOCAL_LOGS=0 path-folder/databasename/runner.sh >> /tmp/wr/databasename/runner.log 2>&1

sudo crontab -l
```
or you can test first before schedule
``` bash
sudo env -i SHELL=/bin/bash PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" LOG_DIR=/tmp/wr/databasename LOCK_DIR=/var/lock/wr/databasename DROP_AFTER_MERGE=1 PURGE_STAGING_OBJECTS=0 USE_DEBUG=0 KEEP_LOCAL_LOGS=0 /bin/bash -lc 'path-folder/databasename/runner.sh'
```
