import json
import mysql.connector


class Job(dict):
    # Calculate properties that are not shown in input file.
    def cal(self):
        self['runTime'] = self['Event_Time'] - self['startTime']
        return
    # cal

    def job2json(self, file):
        new_json = json.dumps(self, sort_keys=True, indent=4)
        print(new_json)
        file.write(new_json)
        return new_json
    # job2json

    def job2sql(self, cursor):
        insert_command = """
                         INSERT INTO jobs
                         (jobId, userId, submitTime, beginTime, termTime)
                         values
                         (%d, %d, %d, %d, %d)
                         """
        cursor.execute(insert_command)
        return
    # job2sql

    def job2jssim(self, file):
        new_line = (str(self['jobId']) + ' '
                    + str(self['userId']) + ' '
                    + str(self['submitTime']) + ' '
                    + str(self['beginTime']) + ' '
                    + str(self['termTime']) + ' '
                    + str(self['runTime']) + ' '
                    + str(self['numProcessors']))
        file.write(new_line)
        file.write("\n")
# Log


# A generator for each property
def gen(arr):
    for i in range(100000):
        yield arr[i]
# gen


# Get stat "prop" for job
def getstat(job, log, prop, prop_type):
    value = log.__next__()

    # handle string: legal string starts and ends with '"'
    if prop_type == str:
        if value[0] == '"':
            while value.count('"') % 2 == 1:
                value = value + ' ' + log.__next__()
            value = value[1:-1]

    # handle integer
    elif prop_type == int:
        value = int(value)

    # handle float
    elif prop_type == float:
        value = float(value)

    else:
        raise Exception

    # final write
    job[prop] = value
    print("prop", prop, "value", value)
    return
# getstat


def handler():
    # Initialization.
    filename = input('Input file name\n')
    infile = open(filename, 'r')

    # Init for json output.
    json_outfile = open('json_out.json', 'w', encoding='utf-8')

    # Init for jssim output.
    jssim_outfile = open('jssim.in', 'w', encoding='utf-8')

    # Init for mysql output.
    sql_config = {
        'user': 'root',
        'password': 'password',
        'host': '127.0.0.1',
        'database': 'test'
    }

    sql_job_table = (
        'CREATE TABLE IF NOT EXISTS jobs ('
        'Event_Type TEXT,'
        'Version_Number TEXT,'
        'Event_Time INT,'
        'jobId INT,'
        'userId INT,'
        'options INT,'
        'numProcessors INT,'    
        'submitTime INT,'
        'beginTime INT,'
        'termTime INT,'
        'startTime INT,'
        'userName TEXT,'
        'queue TEXT,'
        'resReq TEXT,'
        'dependCond TEXT,'
        'preExecCmd TEXT,'
        'fromHost TEXT,'
        'cwd TEXT,'
        'PRIMARY KEY (Event_Time)'
        ')'
    )

    'inFile TEXT,'
    'outFile TEXT,'
    'errFile TEXT,'
    'jobFile TEXT,'
    'numAskedHosts INT,'
    # 'askedHosts'# How to handle THIS?!#
    'numExHosts INT,'
    # 'execHosts'# And how to handle THIS?!#
    'jStatus INT,'
    'hostFactor FLOAT,'
    'jobName TEXT,'
    'command TEXT,'
    'ru_utime FLOAT,'
    'ru_stime FLOAT,'
    'ru_maxrss FLOAT,'
    'ru_ixass FLOAT,'
    'ru_ismrss FLOAT,'
    'ru_idrss FLOAT,'
    'ru_isrss FLOAT,'
    'ru_minflt FLOAT,'
    'ru_majflt FLOAT,'
    'ru_nswap FLOAT,'
    'ru_inblock FLOAT,'
    'ru_outblock FLOAT,'
    'ru_ioch FLOAT,'
    'ru_msgsnd FLOAT,'
    'ru_msgrcv FLOAT,'
    'ru_nsignals FLOAT,'
    'ru_nvcsw FLOAT,'
    'ru_nivcsw FLOAT,'
    'ru_exutime FLOAT,'
    'mailUser TEXT,'
    'projectName TEXT,'
    'exitStatus INT,'
    'maxNumProcessors INT,'
    'loginShell TEXT,'
    'timeEvent TEXT,'
    'idx INT,'
    'maxRMem INT,'
    'maxRSwap INT,'
    'inFileSpool TEXT,'
    'commandSpool TEXT,'
    'rsvId TEXT,'
    'sla TEXT,'
    'exceptMask INT,'
    'additionalInfo TEXT,'
    'exitInfo INT,'
    'warningAction TEXT,'
    'warningTimePeriod INT,'
    'chargedSAAP TEXT,'
    'licenseProject TEXT,'
    'app TEXT,'
    'postExecCmd TEXT,'
    'runTimeEstimation INT,'
    'jubGroupName TEXT,'
    'requeueEvalues TEXT,'
    'option2 INT,'
    'resizeNotifyCmd TEXT,'
    'lastResizeTime INT,'
    'rsvId2 TEXT,'
    'jobDescription TEXT,'
    'submiVARCHARNum INT,'
    'submiVARCHARKey TEXT,'
    'submiVARCHARValue TEXT,'
    'numHostRusage INT,'

    sql_con = mysql.connector.connect(**sql_config)
    sql_cur = sql_con.cursor()

    sql_cur.execute(sql_job_table)

    # Ignore the 1st line of file, as it is not data.
    next(infile)

    counter = 0
    # Loop to deal with each line.
    for line in infile:
        counter += 1
        print("JOB no", counter)
        log_line = gen(line.split())
        job = Job()

        getstat(job, log_line, 'Event_Type', str)
        getstat(job, log_line, 'Version_Number', str)
        getstat(job, log_line, 'Event_Time', int)
        getstat(job, log_line, 'jobId', int)
        getstat(job, log_line, 'userId', int)
        getstat(job, log_line, 'options', int)
        getstat(job, log_line, 'numProcessors', int)
        getstat(job, log_line, 'submitTime', int)
        getstat(job, log_line, 'beginTime', int)
        getstat(job, log_line, 'termTime', int)
        getstat(job, log_line, 'startTime', int)
        getstat(job, log_line, 'userName', str)
        getstat(job, log_line, 'queue', str)
        getstat(job, log_line, 'resReq', str)
        getstat(job, log_line, 'dependCond', str)
        getstat(job, log_line, 'preExecCmd', str)
        getstat(job, log_line, 'fromHost', str)
        getstat(job, log_line, 'cwd', str)
        getstat(job, log_line, 'inFile', str)
        getstat(job, log_line, 'outFile', str)
        getstat(job, log_line, 'errFile', str)
        getstat(job, log_line, 'jobFile', str)
        getstat(job, log_line, 'numAskedHosts', int)

        # using numAskedHosts
        for i in range(job['numAskedHosts']):
            getstat(job, log_line, 'askedHosts' + str(i), str)

        getstat(job, log_line, 'numExHosts', int)

        for i in range(job['numExHosts']):
            getstat(job, log_line, 'execHosts' + str(i), str)

        getstat(job, log_line, 'jStatus', int)
        getstat(job, log_line, 'hostFactor', float)
        getstat(job, log_line, 'jobName', str)
        getstat(job, log_line, 'command', str)
        getstat(job, log_line, 'ru_utime', float)
        getstat(job, log_line, 'ru_stime', float)
        getstat(job, log_line, 'ru_maxrss', float)
        getstat(job, log_line, 'ru_ixrss', float)
        getstat(job, log_line, 'ru_ismrss', float)
        getstat(job, log_line, 'ru_idrss', float)
        getstat(job, log_line, 'ru_isrss', float)
        getstat(job, log_line, 'ru_minflt', float)
        getstat(job, log_line, 'ru_majflt', float)
        getstat(job, log_line, 'ru_nswap', float)
        getstat(job, log_line, 'ru_inblock', float)
        getstat(job, log_line, 'ru_oublock', float)
        getstat(job, log_line, 'ru_ioch', float)
        getstat(job, log_line, 'ru_msgsnd', float)
        getstat(job, log_line, 'ru_msgrcv', float)
        getstat(job, log_line, 'ru_nsignals', float)
        getstat(job, log_line, 'ru_nvcsw', float)
        getstat(job, log_line, 'ru_nivcsw', float)
        getstat(job, log_line, 'ru_exutime', float)
        getstat(job, log_line, 'mailUser', str)
        getstat(job, log_line, 'projectName', str)
        getstat(job, log_line, 'exitStatus', int)
        getstat(job, log_line, 'maxNumProcessors', int)
        getstat(job, log_line, 'loginShell', str)
        getstat(job, log_line, 'timeEvent', str)
        getstat(job, log_line, 'idx', int)
        getstat(job, log_line, 'maxRMem', int)
        getstat(job, log_line, 'maxRSwap', int)
        getstat(job, log_line, 'inFileSpool', str)
        getstat(job, log_line, 'commandSpool', str)
        getstat(job, log_line, 'rsvId', str)
        getstat(job, log_line, 'sla', str)
        getstat(job, log_line, 'exceptMask', int)
        getstat(job, log_line, 'additionalInfo', str)
        getstat(job, log_line, 'exitInfo', int)
        getstat(job, log_line, 'warningAction', str)
        getstat(job, log_line, 'warningTimePeriod', int)
        getstat(job, log_line, 'chargedSAAP', str)
        getstat(job, log_line, 'licenseProject', str)
        getstat(job, log_line, 'app', str)
        getstat(job, log_line, 'postExecCmd', str)
        getstat(job, log_line, 'runTimeEstimation', int)
        getstat(job, log_line, 'jubGroupName', str)
        getstat(job, log_line, 'requeueEvalues', str)
        getstat(job, log_line, 'option2', int)
        getstat(job, log_line, 'resizeNotifyCmd', str)
        getstat(job, log_line, 'lastResizeTime', int)
        getstat(job, log_line, 'rsvId2', str)
        getstat(job, log_line, 'jobDescription', str)
        getstat(job, log_line, 'submiVARCHARNum', int)
        getstat(job, log_line, 'submiVARCHARKey', str)
        getstat(job, log_line, 'submiVARCHARValue', str)
        getstat(job, log_line, 'numHostRusage', int)
        job.cal()
        job.job2json(json_outfile)
        job.job2sql(sql_cur)
        job.job2jssim(jssim_outfile)

    sql_con.close()
    return
# handler

if __name__ == "__main__":
    handler()
