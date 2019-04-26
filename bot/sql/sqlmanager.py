import pandas as pd
import pyodbc

class SqlManager(object):
    def __init__(self,driver,server,db,trusted_connection = True,user=None,pwd=None):
        if trusted_connection == True:
            self.conn_str = ';'.join(['DRIVER={' + driver + '}', 'SERVER=' + server, 'DATABASE=' + db, 'Trusted_Connection=yes'])
        elif user != None and pwd != None:
            self.conn_str = ';'.join(['DRIVER={' + driver + '}', 'SERVER=' + server, 'DATABASE=' + db, 'UID=' + user, 'PWD=' + pwd])
        else:
            print('Realy?, You must learn how connect to ms sql server')
            return

    def exec_query(self,query):
        conn = pyodbc.connect(self.conn_str)
        cursor = conn.cursor()

        try:
            cursor.execute(query)
            result = cursor.fetchall()
            conn.commit()
            return(pd.DataFrame(result))
        except pyodbc.Error as error:
            print('Error:',error.args[1])
            return None
        # else:
        #     conn.commit()
        #     return(pd.DataFrame(result))

class SqlAlertDeadlockAnnouncer(object):
    def __init__(self, lock_limit=10):
        self.sqlmanager = SqlManager()
        self.day_counter = 0
        self.counter = 0
        self.lock_limit = lock_limit

    def check_deadlocks(self):
        query = "select count(*) from programs_message where mesage like '%deadlock%' and CONVERT(varchar,mes_date,112) >= CONVERT(varchar,GETDATE(),112)"
        result = self.sqlmanager.exec_query(query)[0][0][0]

        if self.day_counter < result:
            self.counter += result - self.day_counter
            self.day_counter = result
            
        return self.day_counter

    def run(self):
        self.check_deadlocks()

        if self.counter >= self.lock_limit:
            result = 'Выстрелило уже {} дедлоков, пора бы с этим что-нибудь сделать'.format(str(self.counter))
            self.counter = 0
            return result
        else:
            return None