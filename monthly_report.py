import cx_Oracle
import pandas as pd
import numpy as np
from collections import Counter
import collections as c
from statistics import mean
import datetime
from dateutil.relativedelta import relativedelta
import timeit

class reports:

    def __init__(self, query, from_day_prior, to_day_prior):
        self.query = query
        self.from_day_prior = from_day_prior
        self.to_day_prior = to_day_prior


    def query_days_prior(self,days_prior):
        """
        INPUT: Number of months from today's date you want to go back for forward: Example -7 (seven days prior from today) or +1 (one month in the future)
        OUTPUT: A string with the correct ORACLE SQL formatting you need to run query
        """
        return (datetime.date.today() + relativedelta(days=days_prior)).strftime("%d-%b-%Y").upper()


    def final_query(self):
        """
        INPUT: Nothing: set to query based on todays date (0) to seven days prior (-7).  Date of query baesd on create_date from shipment_orders table in FS_REPORT
        OUTPUT: Final query with adjusted dates
        """
        date_range = "to_date("+"'"+self.query_days_prior(self.to_day_prior)+"'"+") and to_date("+"'"+self.query_days_prior(self.from_day_prior)+"'"+")"
        query_w_o_dates = self.query

        return query_w_o_dates[:-1] + date_range


    def launch(self):
        """
        INPUT: None: querys the database based on dates specified in final_query
        OUTPUT: A pandas dataframe of order_number, create_date, and carrier_name
        """
        con = cx_Oracle.connect('FINAL_REPORTS/password@localhost:portnumber/DATABASENAME')
        cur = con.cursor()

        cur.execute(self.final_query())

        order_number = []
        create_date = []
        account_name = []

        for result in cur:
            order_number.append(list(result)[0])
            create_date.append(list(result)[1])
            account_name.append(list(result)[2])

        df = pd.DataFrame({'shipment_order_id':order_number,
                            'create_date': create_date,
                            'account_name': account_name})
        cur.close()
        con.close()

        order_number = []
        carrier_name = []
        create_date = []
        account_name = []

        return df

class analysis:

    def __init__(self,df):
        self.df = df

    def converting_time(self):
        """
        INPUT: None
        OUTPUT: Takes queried Dataframe and changes datetimes for specificed columns, also reindexs it
        """

        ## Changing date columns to pandas datetime format
        create_date = list(pd.to_datetime(self.df.create_date, dayfirst=True, yearfirst=True, exact=False))

        ## Recasting the columns as pandas datetime
        self.df['create_date'] = create_date

        s_d = []
        for d in self.df.create_date:
            s_d.append(d.date())

        self.df['create_date'] = s_d
        r, c = self.df.shape
        df = self.df.set_index(np.arange(0,r))
        return df

    def by_day(self,column):
        """
        INPUT: Name of the column you want to be the index of the pivot table
        OUTPUT: A data frame of binned shipment orders on a given day based on queried dataframe
        """
        df = self.converting_time()
        return pd.pivot_table(df,index=[column], values=['shipment_order_id'], aggfunc=[len], fill_value=0)

    def by_month(self,column):
        """
        INPUT: Name of the column you want to be the index of the pivot table
        OUTPUT: A data frame of binned shipment orders on a given day based on queried dataframe
        """
        df = self.converting_time()
        return pd.pivot_table(df,index=[column], columns=['create_date'], values=['shipment_order_id'], aggfunc=[len], fill_value=0)

    def save_it(self,time_window,column,name_of_file):
        """
        INPUT: Column from the group by function, and the name you want the file to be called
        OUTPUT: A csv file based on the groupby_column function
        """
        f_name = "/path/to/the/file/"+name_of_file+".csv"

        if time_window == 'by_day':
            return self.by_day(column).to_csv(f_name, encoding='utf-8')
        else:
            return self.by_month(column).to_csv(f_name, encoding='utf-8')

if __name__ == "__main__":
    #Query for the database
    first_query = "SELECT shipment_order_id, create_date, account_name FROM database.table_name WHERE create_date BETWEEN  "

    #Queries data base and stores results
    if datetime.date.today().day == 28:
        df = reports(first_query,0,-27).launch()
    elif datetime.date.today().day == 29:
        df = reports(first_query,0,-28).launch()
    elif datetime.date.today().day == 30:
        df = reports(first_query,0,-29).launch()
    elif datetime.date.today().day == 31:
        df = reports(first_query,0,-30).launch()
    else:
        pass

    #Peforms binning on queried database
    analysis(df).save_it('by_day','account_name','day_count')
    analysis(df).save_it('by_month','account_name','month_count')
    print ("Complete")
