import wx
import fpformat


def EpicsFunction(f):
    """decorator to wrap function in a wx.CallAfter() so that
    Epics calls can be made in a separate thread, and asynchronously.

    This decorator should be used for all code that mix calls to
    wx and epics."""
    def wrapper(*args, **kwargs):
        wx.CallAfter(f, *args, **kwargs)
    return wrapper

class closure:
    """A very simple callback class to emulate a closure (reference to
    a function with arguments) in python.

    This class holds a user-defined function to be executed when the
    class is invoked as a function.  This is useful in many situations,
    especially for 'callbacks' where lambda's are quite enough.
    Many Tkinter 'actions' can use such callbacks.

    >>>def my_action(x=None):
    ...        print 'my action: x = ', x
    >>>c = closure(my_action,x=1)
    ..... sometime later ...
    >>>c()
     my action: x = 1
    >>>c(x=2)
     my action: x = 2

    based on Command class from J. Grayson's Tkinter book.
    """
    def __init__(self,func=None,*args, **kw):
        self.func  = func
        self.kw    = kw
        self.args  = args
    def __call__(self,  *args, **kw):
        self.kw.update(kw)
        if (self.func == None): return None
        self.args = args
        return apply(self.func,self.args,self.kw)

def set_float(val,default=None):
    """ utility to set a floating value, useful for converting from strings """
    if val in (None,''): return default
    try:
        return float(val)
    except:
        return default

class FloatCtrl(wx.TextCtrl):
    """ Numerical Float Control::
      a wx.TextCtrl that allows only numerical input, can take a precision argument
      and optional upper / lower bounds
    """
    def __init__(self, parent, value='', min='', max='', 
                 action=None,  precision=3, action_kw={}, **kwargs):
        
        self.__digits = '0123456789.-'
        self.__prec   = precision
        if precision is None: self.__prec = 0
        self.format   = '%%.%if' % self.__prec
        
        self.__val = set_float(value)
        self.__max = set_float(max)
        self.__min = set_float(min)

        self.fgcol_valid   ="Black"
        self.bgcol_valid   ="White"
        self.fgcol_invalid ="Red"
        self.bgcol_invalid =(254,254,80)
        self.fgcol_inactive =(160,160,160)

        # set up action 
        self.__action = closure()  
        if callable(action):  self.__action.func = action
        if len(action_kw.keys())>0:  self.__action.kw = action_kw

        this_sty =  wx.TE_PROCESS_ENTER|wx.TE_RIGHT
        kw = kwargs
        if kw.has_key('style'): this_sty = this_sty | kw['style']
        kw['style'] = this_sty
            
        wx.TextCtrl.__init__(self, parent, wx.ID_ANY, **kw)        

        self.__CheckValid(self.__val)
        self.SetValue(self.__val)
              
        self.Bind(wx.EVT_CHAR, self.onChar)
        # self.Bind(wx.EVT_CHAR, self.CharEvent)        
        self.Bind(wx.EVT_TEXT, self.onText)

        self.Bind(wx.EVT_SET_FOCUS,  self.onSetFocus)
        self.Bind(wx.EVT_KILL_FOCUS, self.onKillFocus)
        self.Bind(wx.EVT_SIZE, self.onResize)
        self.__GetMark()

    def SetAction(self,action,action_kw={}):
        self.__action = closure()  
        if callable(action):         self.__action.func = action
        if len(action_kw.keys())>0:  self.__action.kw = action_kw
        
    def SetPrecision(self,p):
        if p is None: p = 0
        self.__prec = p
        self.format = '%%.%if' % p
        
    def __GetMark(self):
        " keep track of cursor position within text"
        try:
            self.__mark = min(wx.TextCtrl.GetSelection(self)[0],
                              len(wx.TextCtrl.GetValue(self).strip()))
        except:
            self.__mark = 0

    def __SetMark(self,m=None):
        " "
        if m==None: m = self.__mark
        self.SetSelection(m,m)

    def SetValue(self,value=None,act=True):
        " main method to set value "

        if value == None: value = wx.TextCtrl.GetValue(self).strip()
        self.__CheckValid(value)
        self.__GetMark()
        if self.__valid:
            self.__Text_SetValue(self.__val)
            self.SetForegroundColour(self.fgcol_valid)
            self.SetBackgroundColour(self.bgcol_valid)
            if  callable(self.__action) and act:  self.__action(value=self.__val)
        else:
            self.__val = self.__bound_val
            self.__Text_SetValue(self.__val)
            self.__CheckValid(self.__val)
            self.SetForegroundColour(self.fgcol_invalid)
            self.SetBackgroundColour(self.bgcol_invalid)
            wx.Bell()
        self.__SetMark()
        
    def onKillFocus(self, event):
        value = wx.TextCtrl.GetValue(self).strip()        
        self.__CheckValid(value)
        if self.__valid and callable(self.__action):
            self.__action(value=self.__val)
        event.Skip()

    def onResize(self, event):
        event.Skip()
        
    def onSetFocus(self, event=None):
        self.__SetMark()
        if event: event.Skip()
      
    def onChar(self, event):
        """ on Character event"""
        key   = event.GetKeyCode()
        entry = wx.TextCtrl.GetValue(self).strip()
        pos   = wx.TextCtrl.GetSelection(self)
        # really, the order here is important:
        # 1. return sends to ValidateEntry
        if key == wx.WXK_RETURN:
            self.SetValue(entry)
            return

        # 2. other non-text characters are passed without change
        if (key < wx.WXK_SPACE or key == wx.WXK_DELETE or key > 255):
            event.Skip()
            return
        
        # 3. check for multiple '.' and out of place '-' signs and ignore these
        #    note that chr(key) will now work due to return at #2
        
        has_minus = '-' in entry
        ckey = chr(key)
        if ((ckey == '.' and (self.__prec == 0 or '.' in entry) ) or
            (ckey == '-' and (has_minus or  pos[0] != 0)) or
            (ckey != '-' and  has_minus and pos[0] == 0)):
            return
        # 4. allow digits, but not other characters
        if (chr(key) in self.__digits):
            event.Skip()
            return
        # return without event.Skip() : do not propagate event
        return
        
    def onText(self, event=None):
        try:
            if event.GetString() != '':
                self.__CheckValid(event.GetString())
        except:
            pass
        event.Skip()
        
    def GetValue(self):
        if self.__prec > 0:
            return set_float(fpformat.fix(self.__val, self.__prec))
        else:
            return int(self.__val)
    def GetMin(self):  return self.__min
    def GetMax(self):  return self.__max
    def SetMin(self,min): self.__min = set_float(min)
    def SetMax(self,max): self.__max = set_float(max)

    def SetMinMax(self,min,max):
        self.__min = set_float(min)
        self.__max = set_float(max)        
    
    def __Text_SetValue(self,value):
        wx.TextCtrl.SetValue(self, self.format % set_float(value))
        self.Refresh()
    
    def __CheckValid(self,value):
        # print ' Check valid ', value
        v = self.__val
        try:
            self.__valid = True
            v = set_float(value)
            if self.__min != None and (v < self.__min):
                self.__valid = False
                v = self.__min
            if self.__max != None and (v > self.__max):
                self.__valid = False
                v = self.__max
        except:
            self.__valid = False
        self.__bound_val = v
        if self.__valid:
            self.__bound_val = self.__val = v
            self.SetForegroundColour(self.fgcol_valid)
            self.SetBackgroundColour(self.bgcol_valid)
        else:
            self.SetForegroundColour(self.fgcol_invalid)
            self.SetBackgroundColour(self.bgcol_invalid)            
        self.Refresh()
