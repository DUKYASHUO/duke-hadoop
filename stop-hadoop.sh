#/bin/bash
bash /usr/local/hadoop/sbin/mr-jobhistory-daemon.sh stop historyserver
bash /usr/local/hadoop/sbin/stop-yarn.sh
bash /usr/local/hadoop/sbin/stop-dfs.sh
