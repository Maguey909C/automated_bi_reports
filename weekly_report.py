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
        INPUT: Nothing: set to query based on todays date (0) to seven days prior (-7).  Date of query baesd on shipment_date from shipment_orders table in FS_REPORT
        OUTPUT: Final query with adjusted dates
        """
        date_range = "to_date("+"'"+self.query_days_prior(self.to_day_prior)+"'"+") and to_date("+"'"+self.query_days_prior(self.from_day_prior)+"'"+")"
        query_w_o_dates = self.query

        return query_w_o_dates[:-1] + date_range


    def launch(self):
        """
        INPUT: None: querys the database based on dates specified in final_query
        OUTPUT: A pandas dataframe of order_number, shipment_date, and carrier_name
        """
        con = cx_Oracle.connect('FINAL_REPORTS/password@localhost:portnumber/DATABASENAME')
        cur = con.cursor()

        if 'ship_type' in self.final_query():
            cur.execute(self.final_query())

            order_number = []
            carrier_name = []
            shipment_date = []
            account_name = []
            ship_type = []

            for result in cur:
                order_number.append(list(result)[0])
                carrier_name.append(list(result)[1])
                shipment_date.append(list(result)[2])
                account_name.append(list(result)[3])
                ship_type.append(list(result)[4])

            df = pd.DataFrame({'shipment_order_id':order_number,
                                'carrier_name':carrier_name,
                                'shipment_date': shipment_date,
                                'account_name': account_name,
                                'ship_type': ship_type})
            cur.close()
            con.close()

            order_number = []
            carrier_name = []
            shipment_date = []
            account_name = []
            ship_type = []

            return df

        if 'created_by' in self.final_query():
            cur.execute(self.final_query())

            order_number = []
            carrier_name = []
            shipment_date = []
            account_name = []
            created_by = []
            create_date = []

            for result in cur:
                order_number.append(list(result)[0])
                carrier_name.append(list(result)[1])
                shipment_date.append(list(result)[2])
                account_name.append(list(result)[3])
                created_by.append(list(result)[4])
                create_date.append(list(result)[5])

            df = pd.DataFrame({'shipment_order_id':order_number,
                                'carrier_name':carrier_name,
                                'shipment_date': shipment_date,
                                'account_name': account_name,
                                'created_by': created_by,
                                'create_date': create_date})
            cur.close()
            con.close()

            order_number = []
            carrier_name = []
            shipment_date = []
            account_name = []
            created_by = []
            create_date = []

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
        shipment_date = list(pd.to_datetime(self.df.shipment_date, dayfirst=True, yearfirst=True, exact=False))
        create_date = list(pd.to_datetime(self.df.create_date, dayfirst=True, yearfirst=True, exact=False))

        ## Recasting the columns as pandas datetime
        self.df['shipment_date'] = shipment_date
        self.df['create_date'] = create_date

        s_d = []
        for d in self.df.shipment_date:
            s_d.append(d.date())

        c_d = []
        for d in self.df.create_date:
            c_d.append(d.date())

        self.df['shipment_date'] = s_d
        self.df['create_date'] = c_d

        r, c = self.df.shape
        df = self.df.set_index(np.arange(0,r))

        return df

    def binning_by_date(self, column, df):

        new_df1 = df[(df['shipment_date']< (datetime.date.today()+relativedelta(days=0)))&(df['shipment_date']>(datetime.date.today()+relativedelta(days=-10)))]
        r1, c1 = new_df1.shape
        new_df1['group']=np.ones(r1)

        new_df2 = df[(df['shipment_date']< (datetime.date.today()+relativedelta(days=-11)))&(df['shipment_date']>(datetime.date.today()+relativedelta(days=-40)))]
        r2, c2 = new_df2.shape
        new_df2['group']=np.ones(r2)*2

        new_df3 = df[(df['shipment_date']< (datetime.date.today()+relativedelta(days=-41)))&(df['shipment_date']>(datetime.date.today()+relativedelta(days=-70)))]
        r3, c3 = new_df3.shape
        new_df3['group'] = np.ones(r3)*3

        new_df = pd.concat([new_df1, new_df2, new_df3])

        return new_df

    def groupby_old(self,column,df):
        """
        INPUT: Nothing, will take sliced data frame from binning_by_date
        OUTPUT: A dataframe of shipments that are not delivered and that are old by carrier
        """
        return self.binning_by_date(column,df).groupby(by=['carrier_name', 'group'])['shipment_order_id'].agg(['count'])

    def groupby_user(self,column,df):
        """
        INPUT: Nothing, will take sliced data frame from binning_by_date
        OUTPUT: A dataframe of shipments that are not delivered and that are old by carrier
        """
        return self.binning_by_date(column,df).groupby(by=['account_name','carrier_name','created_by','group'])['shipment_order_id'].agg(['count'])


    def save_it(self,column,name_of_file,df):
        """
        INPUT: Column from the group by function, and the name you want the file to be called
        OUTPUT: A csv file based on the groupby_column function
        """
        f_name = name_of_file+".csv"

        if column == 'shipment_date':
            return analysis(df).groupby_old('shipment_date',df).to_csv(f_name, encoding='utf-8')
        elif column == 'create_date':
            return analysis(df).groupby_user('create_date',df).to_csv(f_name, encoding='utf-8')
        else:
            return 'Column not in orignal table'

if __name__ == "__main__":
    #Query for the database
    second_query = "SELECT shipment_order_id, vendor_name, shipment_date, account_name, created_by, create_date FROM table_name WHERE shipment_date BETWEEN  "

    #Queries data base and stores results
    df = reports(second_query,0,-60).launch()
    df = analysis(df).converting_time()

    #Peforms binning on queried database
    analysis(df).save_it('shipment_date','not_delivered_df',df)
    analysis(df).save_it('create_date','user_df',df)
    print ("Complete")
