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

            for result in cur:
                order_number.append(list(result)[0])
                carrier_name.append(list(result)[1])
                shipment_date.append(list(result)[2])
                account_name.append(list(result)[3])
                created_by.append(list(result)[4])

            df = pd.DataFrame({'shipment_order_id':order_number,
                                'carrier_name':carrier_name,
                                'shipment_date': shipment_date,
                                'account_name': account_name,
                                'created_by': created_by})
            cur.close()
            con.close()

            order_number = []
            carrier_name = []
            shipment_date = []
            account_name = []
            created_by = []

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

        ## Recasting the columns as pandas datetime
        self.df['shipment_date'] = shipment_date

        s_d = []
        for d in self.df.shipment_date:
            s_d.append(d.date())

        self.df['shipment_date'] = s_d
        r, c = self.df.shape
        df = self.df.set_index(np.arange(0,r))
        return df

    def specific_table(self,column):
        """
        INPUT: Column name you want to be in the index of the pivot table
        OUTPUT: A data frame of binned shipment orders on a given day based on queried dataframe
        """
        df = self.converting_time()

        return pd.pivot_table(df,index=[column], columns=['shipment_date'], values=['shipment_order_id'], aggfunc=[len], fill_value=0)

    def offline_table(self,column):
        """
        INPUT: Column name you want to be in the index of the pivot table
        OUTPUT: A data frame of binned shipment orders on a given day based on queried dataframe
        """
        df = self.converting_time()

        return pd.pivot_table(df,index=[column], columns=['ship_type','shipment_date'], values=['shipment_order_id'], aggfunc=[len], fill_value=0)

    def add_missing_companies(self,column):
        """
        INPUT:Column name you want to be the index the pivot table in the function specific table will be
        OUTPUT: A new dataframe of all companies whether they had shipments in the last 7 days or not, specified in the abbie_accounts list.
        Speak with her to verify whether list of companies still needed.
        """
        df = self.specific_table(column)

        specific_accounts = ['name','of','account','name','of','account']

        zero_accounts = list(set(specific_accounts).difference(list(df.index)))

        for name in zero_accounts:
            new_row = pd.DataFrame(data=0, index=[str(name)], columns=df.columns)
            df = df.append(new_row)

        df = df.sort_index()

        return df

    def save_it(self,column,name_of_file):
        """
        INPUT: Column from the group by function, and the name you want the file to be called
        OUTPUT: A csv file based on the groupby_column function
        """
        f_name = "/file/path/folder/"+name_of_file+".csv"
        #f_name2 = "../folder/"+name_of_file+".csv"

        if column == 'account_name':
            return self.add_missing_companies(column).to_csv(f_name, encoding='utf-8')

        elif column == 'carrier_name':
            return self.specific_table(column).to_csv(f_name,encoding='utf-8')
        else:
            return self.offline_table(column).to_csv(f_name,encoding='utf-8')

    def save_it_date(self,column,name_of_file):
        """
        INPUT: Column from the group by function, and the name you want the file to be called
        OUTPUT: A csv file based on the groupby_column function
        """
        f_name = name_of_file+"_" + datetime.date.today().strftime('%Y-%m-%d')+".csv"

        return self.add_missing_companies(column).to_csv(f_name, encoding='utf-8')

if __name__ == "__main__":
    #Query for the database
    first_query = "SELECT shipment_order_id, vendor_name, shipment_date, account_name, ship_type FROM JS_DATABASE.TABLE_NAME WHERE shipment_date BETWEEN  "

    #Queries data base and stores results
    df = reports(first_query,0,-7).launch()

    #Peforms binning on queried database
    #tab1 = analysis(df).groupby_column('carrier_name')
    analysis(df).save_it('carrier_name', 'carrier_df')
    analysis(df).save_it('account_name', 'account_df')
    analysis(df).save_it('ship_type', 'off_on_df')
    print ("Complete")
