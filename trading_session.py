import datetime
import Queue as queue
import time
import pytz
import pandas as pd
from pyvirtualdisplay import Display

from event import EventType, MinuteEvent, TradeTimeEvent, NoTradeTimeEvent
from matrix import Matrix_Logic
from EtoroAPI.Etoro_Interface import Etoro_Interface

class Trade_Detection(object):

    def __init__(self, stocks, market_open_time,
                 market_close_time, events_queue,
                 market_open_window, market_close_window):
        
        self.stocks = stocks
        self.market_open_time = market_open_time
        self.market_close_time = market_close_time
        self.events_queue = events_queue
        self.market_open_window = market_open_window
        self.market_close_window = market_close_window
        self.active_stocks = False
        
        self.current_time = datetime.datetime.now(tz=pytz.timezone('US/Eastern'))
        
    def check_event_open_trades(self):
        '''
        Check the conditions to open a trade in a business day, if succesful then an event is generated.
        The current time has to be in an open time window and self.active_stocks == False 
        '''
        self.update_current_time()

        if self.is_business_day(self.current_time.date()) and \
        self.current_time.time()>self.market_open_time and self.current_time.time()<self.market_open_window:
            if self.active_stocks == False:
                open_trade_stocks = TradeTimeEvent(self.stocks, self.current_time)
                self.events_queue.put(open_trade_stocks)
                self.active_stocks = True
                print 'Open trade event detected'
        
    def check_event_close_trades(self):
        '''
        Check the conditions to close a trade in a business day, if succesful then an event is generated.
        The current time has to be in a close time window and self.active_stocks == True 
        '''
        self.update_current_time()
                
        if self.is_business_day(self.current_time.date()) and \
        self.current_time.time()<self.market_close_time and self.current_time.time()>self.market_close_window:
            if self.active_stocks == True:
                close_trade_stocks = NoTradeTimeEvent(self.stocks, self.current_time)
                self.events_queue.put(close_trade_stocks)
                self.active_stocks = False
                print 'Close trade event detected'
                
    def update_current_time(self):
        self.current_time = datetime.datetime.now(tz=pytz.timezone('US/Eastern'))
    
    def is_business_day(self, date):
        return bool(len(pd.bdate_range(date, date)))

class TradingSession(object):
    
    def __init__(self, stocks, events_queue,
                 market_open_time, market_close_time,
                 market_open_window, market_close_window,
                 matrix_start_date, session_type='live',
                 end_session_time = None, frequency_update = 21, 
                 matrix_logic = None, detect_trading_times = None,
                 path_driver = None, exec_back = True,
                 delta = 50, money = 10000.0):
        
        self.stocks = stocks
        self.session_type = session_type
        self.events_queue = events_queue
        self.end_session_time = end_session_time
        self.cur_time = None
        
        self.matrix_logic = matrix_logic
        self.market_open_time = market_open_time
        self.market_close_time = market_close_time
        self.market_open_window = market_open_window
        self.market_close_window = market_close_window
        self.matrix_start_date = matrix_start_date
        self.frequency_update = frequency_update
        self.detect_trading_times = detect_trading_times
        self.path_driver = path_driver
        self.exec_back = exec_back
        self.delta = delta
        self.money = money
        
        self._config_session()
        
        if self.session_type == 'live':
            if self.end_session_time is None:
                raise Exception("Must initialize the end session time in live trading")

        if self.exec_back:
            display = Display(visible=0, size=(1920, 1080))
            display.start()
                
        self.Etoro_Interface_m = Etoro_Interface(self.path_driver, stocks = self.stocks)
        
    def _config_session(self):
        """
        Initialization of the classes needed in the
        session
        """
        if self.matrix_logic is None:
            self.matrix_logic = Matrix_Logic(
                self.stocks, self.market_close_time, 
                self.market_open_time, self.matrix_start_date,
                self.events_queue, self.frequency_update, self.delta)
            
        if self.detect_trading_times is None:
            self.detect_trading_times = Trade_Detection(
                self.stocks, self.market_open_time,
                self.market_close_time, self.events_queue,
                self.market_open_window, self.market_close_window)
        
        
    def _continue_loop_condition(self):
        
        if self.session_type == 'live':
            return datetime.datetime.now()<self.end_session_time
        
    def _run_session(self):
        
        """
        Generate an infinite while loop that charges an event queue
        and direct each event to its given action. The loop continues
        until the event queue is empty.
        """
        
        if self.session_type == 'live':
            print "Running a Real Time Session until %s"%(self.end_session_time)
        while self._continue_loop_condition():
            try:
                event = self.events_queue.get(False)
            except queue.Empty:
                #print "No events to handle, waiting for new minute"
                self.new_minute()
            else:
                if event is not None:
                    if event.type == EventType.MINUTE:
                        self.cur_time = event.time
                        print "New minute event ", self.cur_time
                        #check matrix correlation update event
                        self.matrix_logic.check_event_update(self.cur_time)
                        #check open trade time event
                        self.detect_trading_times.check_event_open_trades()
                        #check closing time event
                        self.detect_trading_times.check_event_close_trades()
                        pass
                    
                    elif event.type == EventType.UPDATE_MATRIX:
                        #Update the matrix
                        print "Updating the covariance matrix"
                        self.matrix_logic.update_covariance_matrix(event.to_update)
                        
                    elif event.type == EventType.TIME_TRADE:
                        #Trade the stocks
                        print "Time to trade all the stocks"
                        recommended_positions = self.matrix_logic.get_recommended_positions(self.money)
                        print "Recommended positions are (are inverted)", -recommended_positions
                        self.Etoro_Interface_m.open_trades(-recommended_positions)
                    
                    elif event.type == EventType.TIME_NO_TRADE:
                        #Close all the positions
                        print "Closing all the positions"
                        self.Etoro_Interface_m.close_trades()
                        
                    else:
                        raise NotImplemented("Uknown event detected, we can not manage it" % event.type)
                
                
    def new_minute(self):
        '''
        Function that creates an event if a new minute has arrived
        '''
        current_time = datetime.datetime.now(tz=pytz.timezone('US/Eastern'))
        time.sleep(1)
        updated_time = datetime.datetime.now(tz=pytz.timezone('US/Eastern'))
                
        if current_time.minute !=  updated_time.minute:
            new_minute_event = MinuteEvent(self.stocks, updated_time)
            self.events_queue.put(new_minute_event)
            self.Etoro_Interface_m.keep_alive()

