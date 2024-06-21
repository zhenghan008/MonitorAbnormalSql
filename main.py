import json
import os.path
import subprocess
import sys
import multiprocessing
from subprocess import Popen, PIPE
import time


work_path = os.path.abspath(os.path.dirname(__file__))


class Monitor:

    def __init__(self):
        self.server_cpu = multiprocessing.cpu_count() * 100
        with open(work_path + "/.config.json") as f:
            config = json.loads(f.read())
            self.mysql_user = config.get("mysql_user")
            self.mysql_passwd = config.get("mysql_passwd")
            self.mysql_port = config.get("mysql_port")
            self.mysql_host = config.get("mysql_host")
            self.mysql_cpu_threshold = config.get("mysql_cpu_threshold")
            self.top_file_interval = config.get("top_file_interval")
            self.judge_times = config.get("judge_times")
            self.cpu_threshold = config.get("cpu_threshold")
            self.zabbix_server = config.get("zabbix_server")
            self.zabbix_port = config.get("zabbix_port")
            self.zabbix_key = config.get("zabbix_key")
            self.sender_server = config.get("sender_server")

    def __is_get_abnormal_sql(self):
        for each_time in range(self.judge_times):
            subprocess.getoutput(f"top -b -n 1 > /tmp/.top1.txt")
            out = subprocess.getoutput(
                "tail -n +8 /tmp/.top1.txt | awk '{a[$NF]+=$9}END{for(k in a)print (a[k]/('''%d'''))*100,k}' | grep \"\\bmysqld\\b\" | cut -d\" \" -f1" % self.server_cpu)
            if float(out) < self.cpu_threshold:
                return False, 0
            if each_time < self.judge_times - 1:
                time.sleep(self.top_file_interval)
        else:
            pid = subprocess.getoutput("tail -n +8 /tmp/.top1.txt | grep \"\\bmysqld\\b\" | awk '{print $1}'")
            return True, pid

    def get_abnormal_sql(self):
        try:
            __is_get, pid = self.__is_get_abnormal_sql()
            if __is_get is True:
                subprocess.getoutput(f"top -Hp {pid} -b -c -n 1 > /tmp/.top-mysql.txt")
                pids = subprocess.getoutput("tail -n +8 /tmp/.top-mysql.txt |  awk '$9 > %d {print $1}' | xargs" % self.mysql_cpu_threshold).split()
                sql = f'''select * from performance_schema.threads where PROCESSLIST_STATE="Sending data" and THREAD_OS_ID in ({','.join(pids)})\G'''
                shell = f'''mysql -u{self.mysql_user} -p\"{self.mysql_passwd}\" -h{self.mysql_host} -P{self.mysql_port} -e \'{sql}\' | grep -v "performance_schema.threads"'''
                process = Popen(shell, stdout=PIPE, stderr=PIPE, shell=True)
                output, _ = process.communicate()
                __res = output.decode("utf8")
                is_report = __res.find(f"Sending data")
                if is_report != -1:
                    yield f'info: {__res}'
                else:
                    yield " "
            else:
                yield " "
        except Exception as e:
            yield f'error: {str(e)}'


if __name__ == '__main__':
    while 1:
        try:
            m = Monitor()
            res = m.get_abnormal_sql()
            for each_mes in res:
                subprocess.getoutput(
                    f"zabbix_sender -z {m.zabbix_server} -p {m.zabbix_port} -s \"{m.sender_server}\" -k {m.zabbix_key} -o \"{each_mes}\"")
            time.sleep(5)
        except KeyboardInterrupt:
            sys.exit(0)
