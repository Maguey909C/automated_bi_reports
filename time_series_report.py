import cx_Oracle
import pandas as pd
import numpy as np
from collections import Counter
from statistics import mean
import datetime
from dateutil.relativedelta import relativedelta
import timeit
import statsmodels.api as sm
import matplotlib.pyplot as plt

class reports:

    def __init__(self, query):
        self.query = query


    def final_query(self):
        """
        INPUT: Nothing: set to query based on todays date (0) to seven days prior (-7).  Date of query baesd on shipment_date from shipment_orders table in FS_REPORT
        OUTPUT: Final query with adjusted dates
        """
        date_range = "to_date("+"'01-JAN-2015'"+") and to_date("+"'"+(datetime.date.today()).strftime("%d-%b-%Y").upper()+"'"+")"+")"
        query_w_o_dates = self.query

        return query_w_o_dates[:-1] + date_range


    def launch(self):
        """
        INPUT: None: querys the database based on dates specified in final_query
        OUTPUT: A pandas dataframe of order_number, shipment_date, and carrier_name
        """
        con = cx_Oracle.connect('table/password@localhost:15800/AGIUAT')
        # within server use con = cx_Oracle.connect('table/password@databasename.address.us-west-1.rds.amazonaws.com:portnumber/databasename')
        cur = con.cursor()

        cur.execute(self.final_query())

        shipment_order_id = []
        net_charge = []
        create_date = []

        for result in cur:
            shipment_order_id.append(list(result)[0])
            net_charge.append(list(result)[1])
            create_date.append(list(result)[2])

        df = pd.DataFrame({'shipment_order_id':shipment_order_id,
                            'net_charge':net_charge,
                            'create_date': create_date})
        cur.close()
        con.close()

        shipment_order_id = []
        net_charge = []
        create_date = []

        return df

class analysis:

    def __init__(self,df):
        self.df = df

    def binning_dates(self):

        df = self.df

        cnt = Counter()

        for d in range(len(df['create_date'])):
            row = self.df['create_date'][d]
            estimated_charge = self.df['net_charge'][d]

            if row.month == 12 and row.year == 2018:
                cnt['2018-12'] += estimated_charge
            elif row.month == 11 and row.year == 2018:
                cnt['2018-11'] += estimated_charge
            elif row.month == 10 and row.year == 2018:
                cnt['2018-10'] += estimated_charge
            elif row.month == 9 and row.year == 2018:
                cnt['2018-09'] += estimated_charge
            elif row.month == 8 and row.year == 2018:
                cnt['2018-08'] += estimated_charge
            elif row.month == 7 and row.year == 2018:
                cnt['2018-07'] += estimated_charge
            elif row.month == 6 and row.year == 2018:
                cnt['2018-06'] += estimated_charge
            elif row.month == 5 and row.year == 2018:
                cnt['2018-05'] += estimated_charge
            elif row.month == 4 and row.year == 2018:
                cnt['2018-04'] += estimated_charge
            elif row.month == 3 and row.year == 2018:
                cnt['2018-03'] += estimated_charge
            elif row.month == 2 and row.year == 2018:
                cnt['2018-02'] += estimated_charge
            elif row.month == 1 and row.year == 2018:
                cnt['2018-01'] += estimated_charge

            elif row.month == 12 and row.year == 2017:
                cnt['2017-12'] += estimated_charge
            elif row.month == 11 and row.year == 2017:
                cnt['2017-11'] += estimated_charge
            elif row.month == 10 and row.year == 2017:
                cnt['2017-10'] += estimated_charge
            elif row.month == 9 and row.year == 2017:
                cnt['2017-09'] += estimated_charge
            elif row.month == 8 and row.year == 2017:
                cnt['2017-08'] += estimated_charge
            elif row.month == 7 and row.year == 2017:
                cnt['2017-07'] += estimated_charge
            elif row.month == 6 and row.year == 2017:
                cnt['2017-06'] += estimated_charge
            elif row.month == 5 and row.year == 2017:
                cnt['2017-05'] += estimated_charge
            elif row.month == 4 and row.year == 2017:
                cnt['2017-04'] += estimated_charge
            elif row.month == 3 and row.year == 2017:
                cnt['2017-03'] += estimated_charge
            elif row.month == 2 and row.year == 2017:
                cnt['2017-02'] += estimated_charge
            elif row.month == 1 and row.year == 2017:
                cnt['2017-01'] += estimated_charge

            elif row.month == 12 and row.year == 2016:
                cnt['2016-12'] += estimated_charge
            elif row.month == 11 and row.year == 2016:
                cnt['2016-11'] += estimated_charge
            elif row.month == 10 and row.year == 2016:
                cnt['2016-10'] += estimated_charge
            elif row.month == 9 and row.year == 2016:
                cnt['2016-09'] += estimated_charge
            elif row.month == 8 and row.year == 2016:
                cnt['2016-08'] += estimated_charge
            elif row.month == 7 and row.year == 2016:
                cnt['2016-07'] += estimated_charge
            elif row.month == 6 and row.year == 2016:
                cnt['2016-06'] += estimated_charge
            elif row.month == 5 and row.year == 2016:
                cnt['2016-05'] += estimated_charge
            elif row.month == 4 and row.year == 2016:
                cnt['2016-04'] += estimated_charge
            elif row.month == 3 and row.year == 2016:
                cnt['2016-03'] += estimated_charge
            elif row.month == 2 and row.year == 2016:
                cnt['2016-02'] += estimated_charge
            elif row.month == 1 and row.year == 2016:
                cnt['2016-01'] += estimated_charge

            elif row.month == 12 and row.year == 2015:
                cnt['2015-12'] += estimated_charge
            elif row.month == 11 and row.year == 2015:
                cnt['2015-11'] += estimated_charge
            elif row.month == 10 and row.year == 2015:
                cnt['2015-10'] += estimated_charge
            elif row.month == 9 and row.year == 2015:
                cnt['2015-09'] += estimated_charge
            elif row.month == 8 and row.year == 2015:
                cnt['2015-08'] += estimated_charge
            elif row.month == 7 and row.year == 2015:
                cnt['2015-07'] += estimated_charge
            elif row.month == 6 and row.year == 2015:
                cnt['2015-06'] += estimated_charge
            elif row.month == 5 and row.year == 2015:
                cnt['2015-05'] += estimated_charge
            elif row.month == 4 and row.year == 2015:
                cnt['2015-04'] += estimated_charge
            elif row.month == 3 and row.year == 2015:
                cnt['2015-03'] += estimated_charge
            elif row.month == 2 and row.year == 2015:
                cnt['2015-02'] += estimated_charge
            elif row.month == 1 and row.year == 2015:
                cnt['2015-01'] += estimated_charge

            else:
                pass

        df = pd.DataFrame.from_dict(cnt, orient='index').reset_index().rename(columns={'index':'mth_yr', 0:'net_charge'}).sort_values('mth_yr')
        df['mth_yr'] = pd.to_datetime(df['mth_yr'])

        return df.set_index('mth_yr')

    def time_series_model(self):

        """
        INPUT:
        OUTPUT: ARIMA model based on
        """

        df = self.binning_dates()

        mod = sm.tsa.ARIMA(df['net_charge'], order=(1,0,1))
        res = mod.fit(disp=True)

        start = datetime.datetime.strptime("2018-01-01", "%Y-%m-%d")
        date_list = [start + relativedelta(months=x) for x in range(0,12)]
        future = pd.DataFrame(index=date_list, columns= df.columns)
        df2 = pd.concat([df, future])

        df2['forecast'] = res.predict(start = 1, end = 48, dynamic= False)
        graph_df = df2[['net_charge', 'forecast']].iloc[-24:]

        return df2, graph_df

    def save_it(self,name_of_file):
        """
        INPUT: Pandas dataframe of estimated charges for future months
        OUTPUT: A csv file that saved the forecasts
        """
        f_name = "/path/to/file/"+name_of_file+".csv"

        df2, graph = self.time_series_model()

        return df2.to_csv(f_name, encoding='utf-8')

    def save_graph(self,name_of_file):

        """
        INPUT: The name desired for the timeseries graph
        OUTPUT: A matplotlib figure of the forecasted timeseries graph
        """
        f_name = "/path/to/file/"+name_of_file+".png"

        df2, graph_df = self.time_series_model()

        ax = graph_df.plot()
        fig = ax.get_figure()

        return fig.savefig(f_name)

if __name__ == "__main__":
    #Query for the database
    first_query = "SELECT sq.shipment_order_id, sq.net_charge, soo.create_date FROM database.table sq, database.table soo WHERE sq.shipment_order_id = soo.shipment_order_id and sq.net_charge IS NOT NULL and sq.shipment_quote_id in (select sd.shipment_quote_id from FS_USER.shipment_detail sd, FS_USER.shipment_order so where sd.shipment_order_id= so.shipment_order_id and so.vendor_id=39 and so.create_date BETWEEN  "

    #Create the dataframe that will be used for parsing
    df = reports(first_query).launch()
    analysis(df).save_it('mach1_estimated_charges_forecast')
    analysis(df).save_graph('mach1_forecast_visual')
