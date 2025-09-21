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

#input to cron
10 */1 * * * path-folder/runners.sh

sudo crontab -l
```
