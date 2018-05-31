#/bin/bash
bash /usr/local/hadoop/sbin/start-dfs.sh
bash /usr/local/hadoop/sbin/start-yarn.sh
bash /usr/local/hadoop/sbin/mr-jobhistory-daemon.sh start historyserver
