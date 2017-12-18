import datetime

import pytz
import pandas as pd
import numpy as np
import pandas_datareader.data as web

from event import EventType, UpdateMatrixEvent
from lib.Data.Data_Quant import Price_Storage, Price_Cleaned, Data_Derivatives, Price_Storage_AV, Price_Storage_IEX
from lib.Strategy.Covariance_Strategy import qrm_cov_pc, Allocator_Cov


class Matrix_Logic(object):
    
    def __init__(
        self, stocks, market_close_time,
        market_open_time, start_date,
        events_queue, frequency_update = 21,
        delta = 50, alphas = None):
        
        self.start_date = start_date #datetime.datetime
        self.market_open_time = market_open_time #datetime.time, tz = None (No need)
        self.market_close_time = market_close_time #datetime.time, tz = None (No need)
        self.freq = frequency_update
        self.stocks = stocks
        self.events_queue = events_queue
        self.latest_COV_update = datetime.datetime(1900,1,1, tzinfo = pytz.timezone('US/Eastern'))
        self.delta = datetime.timedelta(days=delta)
        self.covariance_mat = None        
        self.tmp_CC_returns = None
        self.alphas = alphas
        
    def check_event_update(self, current_datetime):
        
        '''Functions that generates an UpdateMatrixEvent for different situations: 
        1) If today is a day to update the correlation matrix
        2) If the correlation matrix has not been initialized and so computes it respect to the
        last valid period day
        '''
                
        current_time = current_datetime.time() #time is from 'US/Eastern'
        delta_one_day = datetime.timedelta(days=1)
        d_range = pd.bdate_range(self.start_date, current_datetime + delta_one_day, \
                                 freq = str(self.freq) + 'B')
        
        if d_range[-1].date()<current_datetime.date():
            to_update = d_range[-1]
            #print 'case 1'
            #print d_range
        else: 
            to_update = d_range[-2]
            #print 'case 2'
            #print d_range
        
        '''If today is update day, we update the covariance matrix after the close time,
        (and we do it only once)
        '''
        
        if self.covariance_mat is None:
            print "Covariance matrix in none, updating for the date %s" % (to_update.date())
            matrix_event = UpdateMatrixEvent(self.stocks, current_datetime, to_update, self.freq)
            self.events_queue.put(matrix_event)
        else:
            if to_update.date()!=self.latest_COV_update.date():
                print 'Re-updating the covariance matrix for the date %s' %to_update.date()
                matrix_event = UpdateMatrixEvent(self.stocks, current_datetime, to_update, self.freq)
                self.events_queue.put(matrix_event)        
            else:
                print 'Cov Mat do not require to be re-updated'
                
    def update_covariance_matrix(self, to_update):
        
        '''
        Computes the covariance matrix for a given day (to_update.date())
        Parameters
        to_update: Datetime format containing the most recent date to consider in the
        matrix update.
        '''
        
        print "Updating matrix...", to_update
        self.latest_COV_update = to_update
        self.covariance_download(to_update)
        print "Matrix updated for the date ", to_update
        
    def covariance_download(self, to_update):
        
        '''
        Downloads the inverse covariance matrix, where
        to_update : the most recent date of the covariance matrix
        '''
        until_previous_datetime = to_update - self.delta
        start = until_previous_datetime.date()
        end = to_update.date()
        
        print start
        print end

        Example1 = Price_Storage(self.stocks, start, end)
        Cleaned_1 = Price_Cleaned(Example1)
        data_open = Cleaned_1.call_price('Open')
        data_close = Cleaned_1.call_price('Close')
        Der_prices = Data_Derivatives(data_open, data_close)

        self.tmp_CC_returns = Der_prices.CC_returns
        window_cc_returns = self.tmp_CC_returns[-self.freq:]
        window_cc_returns = window_cc_returns.transpose()

        tmp_cov_results = qrm_cov_pc(window_cc_returns, use_cor = True, excl_first = False)
        self.covariance_mat = tmp_cov_results["inv_cov"]
        
        print "The inverse covariance matrix is ", self.covariance_mat
        
    def get_recommended_positions(self, Money):
        
        current_datetime = datetime.datetime.now(tz=pytz.timezone('US/Eastern'))
        
        #Get date of yesterday business day
        end = current_datetime.date() - datetime.timedelta(days=1)
        #If is monday, we get the day of friday
        if self.is_business_day(end) == False:
            end = current_datetime.date() - datetime.timedelta(days=3)
        
        #We compute a start date, one day before the end date
        start = end - datetime.timedelta(days=1)
        
        Allocator = Allocator_Cov(self.stocks)
        
        #Getting the close price for stocks (of yesterday)
        Example1 = Price_Storage(self.stocks, start, end)
        Cleaned_1 = Price_Cleaned(Example1)
        data_close = Cleaned_1.call_price('Close')
        print "Data close of yesterday is", data_close
        
        #Getting today open price for stocks
        #q = web.get_quote_yahoo(self.stocks)
        #val = q.transpose()
        #print val
        #data_open = val.iloc[2:3,:].astype(float)
        #print "Data open of today is ", data_open

        #Getting today open price for stocks
        #q = Price_Storage_AV(self.stocks)
        #data_open = q.get_prices_by_label('open')
        #data_open = data_open.iloc[0,:].astype(float)
        #print "Data open of today is ", data_open

        #Getting today open price for stocks
        q = Price_Storage_IEX(self.stocks)
        data_open = q.get_prices_by_label()
        data_open = data_open.iloc[0,:].astype(float)
        print "Data open of today is ", data_open
        
        #Computing alpha
        ratio = data_open / data_close.iloc[-1,:]
        print "The ratio is ", ratio
        #self.alphas = np.log(ratio.iloc[0])  #Old version for the yahoo quote
        self.alphas = np.log(ratio)
        print "The alphas are ", self.alphas
        
        #Computing positions
        H = Allocator.get_allocations(self.alphas, self.covariance_mat)
        Allocations = np.round(H * Money, 2)

        return Allocations

    def is_business_day(self, date):
        return bool(len(pd.bdate_range(date, date)))
