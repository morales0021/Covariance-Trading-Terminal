# -*- coding: utf-8 -*-
"""
General Quantitative Recherche Library 1

Data loading objects tools
"""

import pandas_datareader.data as web
import pandas as pd
import numpy as np
import multiprocessing


class Price_Storage(object):
    
    def __init__(self, symbols, start_date, end_date, jobs = -1):
        self.symbols = symbols
        self.start_date = start_date
        self.end_date = end_date
        
        self.index = pd.bdate_range(start_date, end_date)
        self.dict_stock_data = {}
        
        if jobs == -1:
            self.load_prices_parallel()
        else:
            self.load_prices()
        
    def __Reader_Web__(self, symbol,out_p):
        aux_dict_stock_data = {}
        df = web.DataReader(symbol,"google", self.start_date, self.end_date)
        aux_dict_stock_data[symbol] = df
        out_p.put(aux_dict_stock_data)
        
        print "Symbol: ", symbol," succesfully loaded"
        
    def load_prices(self):
        
        '''Load prices data in linear mode '''
        
        for symbol in self.symbols:
            df_temp = web.DataReader(symbol, "google", self.start_date, self.end_date)
            self.dict_stock_data[symbol] = df_temp
            print "Symbol: ", symbol," succesfully loaded"
            
        print "Symbols succesfully loaded"
    
    def load_prices_parallel(self):
        
        '''Load price data in a parallel fashion mode'''
        
        jobs = []
        
        out_p = multiprocessing.Queue()
        
        for symbol in self.symbols:
            job = multiprocessing.Process(target = self.__Reader_Web__, args=(symbol,out_p))
            jobs.append(job)
        
        for job in jobs:
            job.start()
        
        for job in jobs:
            aux = out_p.get()
            symbol = aux.keys()[0]
            self.dict_stock_data[symbol]= aux[symbol]
            
        for job in jobs:
            job.join()
        
        print "Symbols succesfully loaded"
    

    def get_prices_by_label(self, price_label):
        
        '''Give a pandas dataframe with the label prices'''
           
        df_label = pd.DataFrame(index=self.index)
            
        for symbol in self.symbols:

            tmp_price = self.dict_stock_data[symbol][price_label]
            df_label = df_label.join(tmp_price)
            df_label = df_label.rename(columns={price_label: symbol})
        
            if df_label.empty:
                print symbol
                pdb.set_trace()
        df_label = df_label.dropna()
            
        return df_label

            


class Price_Cleaned:
    
    def __init__(self, Price_Data):
        
        self.Price_Data = Price_Data
        self.index = 0 #General index for all price data
        self.symbols = Price_Data.symbols

        self.lower_outliers = {}
        self.upper_outliers = {}
        self.index_no_outliers = 0
        
        self.__Uniform_Index() #Computes self.index
        
        
    def __Uniform_Index(self):
        
        'Generates a price data with the same index'
        Open_prices = self.Price_Data.get_prices_by_label('Open')
        Close_prices = self.Price_Data.get_prices_by_label('Close')
        
        idx1 = Open_prices.index
        idx2 = Close_prices.index
        idx_i = idx1.intersection(idx2)
        
        self.index = idx_i


    def __no_outliers_Index(self, symbols, outlier_list):
        
        index_no_outliers = self.index
        
        for outlier_direction in outlier_list: 
        
            for symbol in symbols:
                delete_rows = outlier_direction[symbol].index    
                index_no_outliers = index_no_outliers.difference(delete_rows)
                
        return index_no_outliers
    
        
    def __Bolinger_Bands(self, stock_price, window_size, num_of_std):
        
        rolling_mean = stock_price.rolling(window = window_size).mean()
        rolling_std  = stock_price.rolling(window = window_size).std()
        upper_band = rolling_mean + (rolling_std*num_of_std)
        lower_band = rolling_mean - (rolling_std*num_of_std)

        return rolling_mean, upper_band, lower_band

    
    def get_outliers(self, price_label, window_size = 10, num_of_std = 15):
        
        "Returns the outliers of the price data based on a bollinger bands approach"
        
        tmp_prices = self.call_price(price_label)
        
        M, U_B, L_B = self.__Bolinger_Bands(tmp_prices, window_size, num_of_std)
               
        for symbol in self.symbols:
            self.lower_outliers[symbol] = tmp_prices[symbol].loc[(tmp_prices[symbol] < L_B[symbol].shift(1))]
            self.upper_outliers[symbol] = tmp_prices[symbol].loc[(tmp_prices[symbol] > U_B[symbol].shift(1))]
        
        
    def call_price(self,price_label):
        
        tmp_prices = self.Price_Data.get_prices_by_label(price_label)
        
        return pd.DataFrame(tmp_prices, index=self.index)    

    
    def call_price_no_outliers(self, price_label, price_label_cleaning, symbols, direction, window_size = 10, num_of_std = 15):
        
        self.get_outliers(price_label_cleaning, window_size, num_of_std)
        
        if direction == 'upper':
            outlier_list =  [self.upper_outliers]
            
        elif direction == 'lower':
            outlier_list = [self.lower_outliers]
            
        elif direction == 'both':
            outlier_list = [self.upper_outliers, self.lower_outliers]
            
        else:
            
            print 'Error ! set a valid direction for the cleaning'
            
            return 0
        
        self.index_no_outliers = self.__no_outliers_Index(symbols, outlier_list)
        
        tmp_prices = self.Price_Data.get_prices_by_label(price_label)
        
        return pd.DataFrame(tmp_prices, index=self.index_no_outliers)  


class Data_Derivatives(object):
    
    def __init__(self, data_open, data_close):
        self.Data_Open = data_open
        self.Data_Close = data_close
        
        self.Daily_alphas = np.log( data_open / data_close.shift(1) )  #log of alpha values
        self.CC_returns =  np.log( data_close / data_close.shift(1) )  #log of close to close values
        self.Real_returns =  data_close/data_open  #open to close return values      
