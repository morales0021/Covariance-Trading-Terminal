#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
General Quantitative Recherche Library 2

Commisions objects for algorithmic trading sessions

"""
class Commission_Fixed(object):
    
    def __init__(self, com_per_share = 0.005, min_per_order = 1.00, max_percentage = 0.5):
        
        self.total_commission = 0 #Total commision to pay
        
        self.exec_commission = 0
        self.transaction_cost = 0
        
        self.min_per_order = min_per_order #Dollars
        self.com_per_share = com_per_share #Dollars
        self.max_percentage = max_percentage #Percentage 
        
        self.transac_fee_ratio = 0.0000218
        self.finra_fee_ratio = 0.000119
        self.limit_finra_cost = 5.95
        
    def get_execution_cost(self, price, shares, type_order = 'buy'):
        
        '''Computes self.total_commision = self.exec_commision + self.transaction_cost'''
        
        self.exec_commission = self.com_per_share * shares
        max_per_order = price * self.max_percentage * 0.01 * shares
        
        
        if self.min_per_order < max_per_order:

            if self.exec_commission < self.min_per_order:

                self.exec_commission = 1.00

            elif self.exec_commission > max_per_order:

                self.exec_commission = max_per_order
        else:
            
            if self.exec_commission < self.min_per_order:

                self.exec_commission = 1.00
                
        self.get_transaction_cost(price, shares, type_order)        
        self.total_commission = self.exec_commission + self.transaction_cost
        
        return self.total_commission
        
    def get_transaction_cost(self, price, shares, type_order):
        
        if type_order == 'buy':
            
            self.transaction_cost = 0
            
        else:
            
            finra_cost= self.finra_fee_ratio * price * shares 
            
            self.transaction_cost = self.transac_fee_ratio * price * shares
            
            if finra_cost > self.limit_finra_cost :
                
                self.transaction_cost += self.limit_finra_cost
                
            else :
                
                self.transaction_cost += finra_cost
                
                
class Simulation_Commission(object):
    
    def __init__(self):
        
        self.simulated_commission = 0.0
        
    def get_stock_commission(self, price, shares):
        
        self.price = price
        self.shares = shares
        
        tmp_com = Commission_Fixed()
        
        local_commission = tmp_com.get_execution_cost(self.price,shares,'buy') + \
                        tmp_com.get_execution_cost(self.price,shares,'sell')
        
        return local_commission
    
    def get_portfolio_commission(self, dict_price, dict_shares):
        
        total = 0.0
        
        for stock in dict_price:
            total+=self.get_stock_commission(dict_price[stock], dict_shares[stock])
            
        self.simulated_commission = total
        
        return self.simulated_commission
