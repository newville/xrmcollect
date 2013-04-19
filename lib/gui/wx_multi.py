"""
http://wiki.wxpython.org/MultiProcessing

Simpler wxPython Multiprocessing Example
----------------------------------------

This simple example uses a wx.App to control and monitor a pool of workers
instructed to carry out a list of tasks.

The program creates the GUI plus a list of tasks, then starts a pool of workers
(processes) implemented with a classmethod. Within the GUI, the user can start
and stop processing the tasks at any time.

Copyright (c) 2010, Roger Stuckey. All rights reserved.
"""

import getopt, math, random, sys, time, types, wx

from multiprocessing import Process, Queue, cpu_count, current_process, freeze_support

def worker(input, output):
    """
        Create a TaskProcessor object and calculate the result.
    """
    while True:
        args = input.get()
        time.sleep(0.02)
        print len(args), args, input
        result = 0
        # Calculate the result of a task
        for i in range(args[0]):
            angle_rad = math.radians(args[1])
            result += math.tanh(angle_rad)/math.cosh(angle_rad)/args[0]
  
        # Put the result on the output queue
        output.put(( current_process().name, current_process().pid, args[1], result ))

class MyFrame(wx.Frame):
    """
    A simple Frame class.
    """
    def __init__(self, parent, id, title, processes, taskqueue, donequeue, tasks):
        """
        Initialise the Frame.
        """
        self.processes = processes
        self.numprocesses = len(processes)
        self.taskQueue = taskqueue
        self.doneQueue = donequeue
        self.Tasks = tasks
        self.numtasks = len(tasks)

        wx.Frame.__init__(self, parent, id, title, wx.Point(700, 500), wx.Size(300, 200))

        # Create the panel, sizer and controls
        self.panel = wx.Panel(self, wx.ID_ANY)
        self.sizer = wx.GridBagSizer(5, 5)

        self.start_bt = wx.Button(self.panel, wx.ID_ANY, "Start")
        self.Bind(wx.EVT_BUTTON, self.OnStart, self.start_bt)
        self.start_bt.SetDefault()
        self.start_bt.SetToolTipString('Start the execution of tasks')
        self.start_bt.ToolTip.Enable(True)

        self.output_tc = wx.TextCtrl(self.panel, wx.ID_ANY, style=wx.TE_MULTILINE|wx.TE_READONLY)

        # Add the controls to the sizer
        self.sizer.Add(self.start_bt, (0, 0), flag=wx.ALIGN_CENTER|wx.LEFT|wx.TOP|wx.RIGHT, border=5)
        self.sizer.Add(self.output_tc, (1, 0), flag=wx.EXPAND|wx.LEFT|wx.RIGHT|wx.BOTTOM, border=5)
        self.sizer.AddGrowableCol(0)
        self.sizer.AddGrowableRow(1)

        self.panel.SetSizer(self.sizer)
        self.Bind(wx.EVT_CLOSE, self.OnClose)

        # Set some program flags
        self.keepgoing = True
        self.i = 0
        self.donecount = 0

        self.output_tc.AppendText('Number of processes = %d\n' % self.numprocesses)

    def OnStart(self, event):
        """
        Start the execution of tasks by the processes.
        """
        self.start_bt.Enable(False)
        self.output_tc.AppendText('Unordered results...\n')
        # Start processing tasks
        self.processTasks()
        if self.keepgoing:
            self.start_bt.Enable(True)

    def OnClose(self, event):
        """
        Stop the task queue, terminate processes and close the window.
        """
        if (self.donecount < self.i):
            self.output_tc.AppendText('Completing queued tasks...\n')
        self.start_bt.Enable(False)
        busy = wx.BusyInfo("Waiting for processes to terminate...")
        # Stop processing tasks and terminate the processes
        self.processTerm()
        self.Destroy()

    def processTasks(self):
        """
        Start the execution of tasks by the processes.
        """
        self.keepgoing = True

        # Submit first set of tasks
        numprocstart = min(self.numprocesses, self.numtasks)
        for self.i in range(numprocstart):
            self.taskQueue.put(self.Tasks[self.i])

        self.donecount = -1 # done queue index
        self.i = numprocstart - 1 # task queue index
        while (self.donecount < self.i):
            # Get and print results
            self.donecount += 1
            #output = self.doneQueue.get()
            # Execute some function (Yield to a wx.Button event)
            #if (isinstance(resfunc, (types.FunctionType, types.MethodType))):
            self.update()
            if ((self.keepgoing) and (self.i + 1 < self.numtasks)):
                # Submit another task
                self.i += 1
                print 'Add Task! ', self.i
                self.taskQueue.put(self.Tasks[self.i])

    def update(self):
        """
        Get and print the results from one completed task.
        """
        output = self.doneQueue.get()
        self.output_tc.AppendText('%s [%d] calculate(%d) = %.2f\n' % output)
        # Give the user an opportunity to interact
        wx.YieldIfNeeded()

    def processTerm(self):
        """
        Stop the execution of tasks by the processes.
        """
        self.keepgoing = False

        while (self.donecount < self.i):
            # Get and print any results remining in the done queue
            self.donecount += 1
            self.update() 
        for n in range(self.numprocesses):
            # Terminate any running processes
            self.processes[n].terminate()

        # Wait for all processes to stop
        isalive = 1
        while isalive:
            isalive = 0
            for n in range(self.numprocesses):
                isalive = isalive + self.processes[n].is_alive()
            time.sleep(0.5)




class MyApp(wx.App):
    """
    A simple App class, modified to hold the processes and task queues.
    """
    def __init__(self, redirect=True, filename=None, useBestVisual=False, clearSigInt=True, processes=[ ], taskqueue=[ ], donequeue=[ ], tasks=[ ]):
        """
        Initialise the App.
        """
        self.Processes = processes
        self.taskQueue = taskqueue
        self.doneQueue = donequeue
        self.Tasks = tasks

        wx.App.__init__(self, redirect, filename, useBestVisual, clearSigInt)

    def OnInit(self):
        """
        Initialise the App with a Frame.
        """
        self.frame = MyFrame(None, -1, 'wxSimpler_MP', self.Processes, self.taskQueue, self.doneQueue, self.Tasks)
        self.frame.Show(True)
        return True


if __name__ == '__main__':

    freeze_support()

    numtasks = 10
    # Determine the number of CPU's/cores
    numproc = cpu_count()

    # Create the task list
    Tasks = [ (int(1e6), random.randint(0, 45)) for i in range(numtasks) ]

    # Create the queues
    taskQueue = Queue()
    doneQueue = Queue()

    Processes = [ ]

    # The worker processes must be started here!
    for n in range(numproc):
        process = Process(target=worker, args=(taskQueue, doneQueue))
        process.start()
        Processes.append(process)

    # Create the app, including worker processes
    app = MyApp(redirect=True, filename='wxsimpler_mp.stderr.log', processes=Processes, taskqueue=taskQueue, donequeue=doneQueue, tasks=Tasks)
    app.MainLoop()
