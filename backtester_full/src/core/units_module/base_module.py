from abc import ABC, abstractmethod

class BaseUnitModule(ABC):

    def __init__(self):
        self.data_to_store = {}  # Initialize here
        super().__init__()  
        
    @property
    @abstractmethod
    def holiday_calendar(self):
        pass
    
    @abstractmethod
    def trades_on_date(self, date, portfolio, risks_on_dates):
        pass

    @abstractmethod
    def instrument_on_date(self, date):
        pass

    @abstractmethod
    def risk_dates(self, start_date, end_date):
        pass
    