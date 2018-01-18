import json
import mysql.connector


class Job(dict):
    def job2json(self, file):
        new_json = json.dumps(self, sort_keys=True, indent=4)
        print(new_json)
        file.write(new_json)
        return new_json
    # job2json

    def job2sql(self, cursor):
        cursor.execute("INSERT INTO jobs("
                       "")
        return
    # job2sql
# Log


# A generator for each property
def gen(arr):
    for j in range(10000):
        yield arr[j]
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


# Initialization.
filename = input('Input file name\n')
infile = open(filename, 'r')
outfile = open('json_out.json', 'w', encoding='utf-8')
counter = 0

db = mysql.connector.connect(user='root', database='test', password='password')
cur = db.cursor()
cur.execute("CREATE TABLE jobs("
            "Event_Type TEXT,"
            "Version_Number TEXT,"
            "Event_Time INT,"
            "jobId INT,"
            "userId INT,"
            "options INT,"
            "numProcessors INT,"
            "submitTime INT,"
            "beginTime INT,"
            "termTime INT,"
            "startTime INT,"
            "userName TEXT,"
            "queue TEXT,"
            "resReq TEXT,"
            "dependCond TEXT,"
            "preExecCmd TEXT,"
            "fromHost TEXT,"
            "cwd TEXT,"
            "inFile TEXT,"
            "outFile TEXT,"
            "errFile TEXT,"
            "jobFile TEXT,"
            "numAskedHosts INT"
            ");")


# Ignore the 1st line of file, as it is not data.
next(infile)

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
    getstat(job, log_line, 'submitEXTNum', int)
    getstat(job, log_line, 'submitEXTKey', str)
    getstat(job, log_line, 'submitEXTValue', str)
    getstat(job, log_line, 'numHostRusage', int)
    job.job2json(outfile)
    job.job2sql(cur)
