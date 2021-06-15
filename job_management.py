import datetime
import psycopg2
class Tracker(object):
    """
    job_id, status, updated_time
    """
    def __init__(self, jobname, dbconfig):
        self.jobname = jobname
        self.dbconfig = dbconfig

    def assign_job_id(self):
        # [Construct the job ID and assign to the return variable]
        today = datetime.date.today()
        d1 = today.strftime("%Y-%m-%d")
        job_id = self.jobname + '_' + d1
        return job_id

    def update_job_status(self, status):
        job_id = self.assign_job_id()
        print("Job ID Assigned: {}".format(job_id))
        update_time = datetime.datetime.now()
        table_name = self.dbconfig.get("postgres", "job_tracker_table")
        connection = self.get_db_connection()
        try:
            # [Execute the SQL statement to insert to job status table]
            cursor = connection.cursor()
            sql = 'INSERT INTO {} (job_id, status, updated_time) values ({},{},{}) '.format(table_name, job_id,
                                                                                    self.get_job_status(), update_time)
            cursor.execute(sql)
        except (Exception, psycopg2.Error) as error:
            print("error executing db statement for job tracker.")
        return

    def get_job_status(self, job_id):
        # connect db and send sql query
        table_name = self.dbconfig.get('postgres', 'job_tracker_table')
        connection = self.get_db_connection()
        try:
            # [Execute SQL query to get the record]
            cursor = connection.cursor()
            sql = "select job_id, status, updated_time from {}".format(table_name)
            cursor.execute(sql)
            record = cursor.fetchone()
            return record
        except (Exception, psycopg2.Error) as error:
            print("error executing db statement for job tracker.")
        return

    def get_db_connection(self):
        connection = None
        try:
            # [Initialize database connection]
            connection = psycopg2.connect(database='postgres', user='jordan')
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)
        return connection


