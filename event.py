from enum import Enum

EventType = Enum("EventType", "MINUTE TIME_TRADE TIME_NO_TRADE UPDATE_MATRIX ORDER")

class Event(object):
    
    @property
    def typename(self):
        return self.type.name

    
class MinuteEvent(Event):
    
    def __init__(self, stocks, time):
        """
        Parameters:
        stocks - The list of stocks to trade
        """
        self.type = EventType.MINUTE
        self.stocks = stocks
        self.time = time
        
    def __str__(self):
        return "Type: %s, Stocks: %s, Time: %s" % (
            str(self.type), str(self.stocks), str(self.time)
        )
    
    def __repr__(self):
        return str(self)
    
    
class TradeTimeEvent(Event):
    
    def __init__(self, stocks, time):
        """
        Parameters:
        stocks - The list of stocks to trade
        """
        self.type = EventType.TIME_TRADE
        self.stocks = stocks
        self.time = time
        
    def __str__(self):
        return "Type: %s, Stocks: %s, Time: %s" % (
            str(self.type), str(self.stocks), str(self.time)
        )
    
    def __repr__(self):
        return str(self)
        
class NoTradeTimeEvent(Event):
    
    def __init__(self, stocks, time):
        """
        Parameters:
        stocks - The list of stocks to trade
        """
        self.type = EventType.TIME_NO_TRADE
        self.stocks = stocks
        self.time = time
        
    def __str__(self):
        return "Type: %s, Stocks: %s, Time: %s" % (
            str(self.type), str(self.stocks), str(self.time)
        )
    
    def __repr__(self):
        return str(self)
        
        
class UpdateMatrixEvent(Event):
    
    def __init__(self, stocks, time, to_update, backdays):
        """
        Parameters:
        stocks - The list of stocks to trade
        update_time - Last date for updating the covariance matrix
        backdays - Number of backdays to consider in the covariance matrix
        """
        self.type = EventType.UPDATE_MATRIX
        self.stocks = stocks
        self.time = time
        self.to_update = to_update
        self.backdays = backdays
        
    def __str__(self):
        return "Type: %s, Stocks: %s, Time: %s, To Update: %s, Backdays: %s" % (
            str(self.type), str(self.stocks), str(self.time), str(self.to_update), str(self.backdays)
        )
    
    def __repr__(self):
        return str(self)
        
        
class OrderEvent(Event):
    
    def __init__(self, stocks, time, action, positions):
        """
        Parameters:
        action - 'OPEN' (for opening positions) or 'CLOSE' (for closing positions)
        stocks - The list of stocks to trade
        positions - Dictionary of the stocks positions
        """
        self.type = EventType.ORDER
        self.action = action
        self.stocks = stocks
        self.time = time
        self.positions = positions
        
    def __str__(self):
        return "Type: %s, Stocks: %s, Time: %s, Action: %s, Positions: %s" % (
            str(self.type), str(self.stocks), str(self.time), str(self.action), str(self.positions)
        )
    
    def __repr__(self):
        return str(self)