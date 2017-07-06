#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 27 22:18:33 2017

@author: jmorales
"""
import numpy as np
from lib.Data.Data_Quant import Price_Storage, Price_Cleaned, Data_Derivatives
from lib.Commission.Commissions import Simulation_Commission

class Round_Allocations(object):
    
    def __init__(self):
        self.H = 0 #Percentage allocation
        self.Money = 0
       
        self.Money_by_H = 0
        
        self.shares_by_stock = 0
        self.R_Money_by_H = 0
        
        self.open_price_today = 0
        
    def round_money(self, H, open_price_today, Money):
        self.open_price_today = open_price_today
        
        self.H = H
        self.Money_by_H = H * Money

        self.shares_by_stock = np.round(self.Money_by_H/open_price_today)  
        self.R_Money_by_H = self.shares_by_stock * open_price_today
        
        return self.R_Money_by_H
        
    def get_price_shares_dict(self):
        self.dict_price = self.open_price_today.to_dict()
        self.dict_shares = self.shares_by_stock.to_dict()
        
        return [self.dict_price, self.dict_shares]


class Allocator_Cov(object):
    
    def __init__(self, symbols):
        
        self.allocations = []
        self.symbols = symbols
        
    def get_allocations(self, expected_return, Inv_Cov):
        '''Returns the weights of the allocations based on an inverse covariance matrix"
        and the expected returns for each symbol'''

        tot_syms = len(self.symbols)
        H = [0] * tot_syms
        Num = 0.0
        Den = 0.0

        for k in range(0, tot_syms):
            for l in range(0, tot_syms):
                Num = Num + Inv_Cov[k,l] * expected_return.iloc[l]
                Den = Den + Inv_Cov[k,l]


        for i in range(0, tot_syms):
            Fir = 0.0
            Sec = 0.0

            for j in range(0, tot_syms):
                Fir = Fir + (Inv_Cov[i,j] * expected_return.iloc[j])
                Sec = Sec + (Inv_Cov[i,j] * (Num/Den))

            H[i] = Fir - Sec

        H_abs = map(abs,H)
        H = -np.array( H/sum(H_abs) )

        self.allocations = H
        
        return self.allocations


class Covariance_Strategy(object):
    
    def __init__(self, data_open, data_close, Money, days):
        
        self.data_open = data_open
        self.data_close = data_close
        self.symbols = list(data_open.keys())
        self.Money = Money
        self.d = days
        
        #Obtain data derivatives
        Der_prices = Data_Derivatives(data_open, data_close)
        self.Daily_alphas = Der_prices.Daily_alphas
        self.CC_returns = Der_prices.CC_returns
        self.Real_returns = Der_prices.Real_returns
        
        self.tot_val = len(self.Daily_alphas)
        
        #Generate allocator strategy object
        self.Allocator = Allocator_Cov(self.symbols)
        
        #Generate round allocator object
        self.H_object = Round_Allocations()
        
        #Generate commision object
        
        self.obj_commission = Simulation_Commission()
        
        
        self.Total_Return = []
        
    def execute(self):
        
        d = self.d
        
        day_list = range(0,d)
        Windows = range(0,self.tot_val, d)

        for window_time in Windows[:-1]:

            window_cc_returns = self.CC_returns[window_time : window_time + d]
            window_cc_returns = window_cc_returns.transpose()

            tmp_cov_results = qrm_cov_pc(window_cc_returns, use_cor = True, excl_first = False)
            In_FMCov = tmp_cov_results["inv_cov"]

            if window_time == Windows[-2]:
                residue = self.tot_val - Windows[-1]
                day_list = range(0,residue)
                print residue
                print window_time
                print Windows
                
            for each_day in day_list:

                alphas = self.Daily_alphas.iloc[window_time + d + each_day, :]
                H = self.Allocator.get_allocations(alphas, In_FMCov)
                open_price_today = self.data_open.iloc[window_time + d + each_day, :]
                Allocations = self.H_object.round_money(H, open_price_today, self.Money)
                Returns = sum( (self.Real_returns.iloc[window_time + d + each_day] - 1) * Allocations)\
                - self.commission()

                self.Total_Return.append(Returns)
        
    def commission(self):
        [dict_price, dict_shares]=self.H_object.get_price_shares_dict()
        cost = self.obj_commission.get_portfolio_commission(dict_price, dict_shares)
        return cost
