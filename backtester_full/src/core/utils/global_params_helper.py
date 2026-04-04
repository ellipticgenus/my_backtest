class GlobalParamsHelper:

    def __init__(self):
        self._params = {}  
    
    def set_param(self, name, value):
        self._params[name] = value
        
    def get_param(self, name):
        return self._params.get(name)
    
    def __setitem__(self, name, value):
        self.set_param(name, value)
        
    def __getitem__(self, name):
        return self.get_param(name)
    
    
GLOBALPARAMS = GlobalParamsHelper()