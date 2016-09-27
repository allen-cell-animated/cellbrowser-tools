# -*- coding: utf-8 -*-
"""
Created on Mon Sep 26 2016

@author: DMT
"""

import sys,os

import upy
upy.setUIClass()

from upy import uiadaptor
import re
import c4d

#get the helperClass for modeling
helperClass = upy.getHelperClass()

class VtkSegmentationReader(uiadaptor):
    def setup(self,vi=None):
        #get the helper
        self.helper = helperClass(vi=vi)
        #dont want to dock it ie maya
        self.dock = False
        #initialize widget and layout
        self.initWidget()
        self.setupLayout()

    #theses two function are for c4d
    def CreateLayout(self):
        self._createLayout()
        return 1
    def Command(self,*args):
#        print args
        self._command(args)
        return 1

    def loadVTK_bSplines(self, filename, objprefix, scale=0.1):
        if not filename : return
        vtkname,ext=os.path.splitext(os.path.basename(filename))
        print filename

        f = open(filename)
        lines = f.readlines()
        f.close()
        # print "closed file"

        vertexList = []
        edgeList = []

        lineNr = 0
        pattern = re.compile('([\d]+) ([\d]+) ([\d]+)')
        # print "ok 0"
        # read up to the POINTS string
        while "POINTS" not in lines[lineNr]:
            lineNr += 1
        # consume the POINTS line
        lineNr += 1
        # print "ok 1.0"
        # read POINTS up to the LINES string, 3 coordinates at a time
        while "LINES" not in lines[lineNr]:
            line_as_floats = [float(x) for x in lines[lineNr].split()]
            for i in range(len(line_as_floats)/3):
                vertexList.append((line_as_floats[i*3+0], line_as_floats[i*3+1], line_as_floats[i*3+2]))
            lineNr += 1
        # now read LINES to the end:
        # print "ok 1.1"
        linesList = []
        while lineNr < len(lines)-1:
            lineNr += 1
            line_as_ints = [int(x) for x in lines[lineNr].split()]
            # print len(line_as_ints)
            # nrOfPoints = line_as_ints[0]
            spline = []
            for i in range(1, len(line_as_ints)):
                spline.append(vertexList[line_as_ints[i]])
            if len(spline) > 1:
                linesList.append(spline)


        scn = self.helper.getCurrentScene()
        # nr = c4d.BaseObject(c4d.Onull)
        # nr.SetName(objprefix+'_splineds')
        # self.helper.addObjectToScene(scn,nr,parent=None)
        nr = self.helper.newEmpty(objprefix+'_splineds', display=1, visible=1)
        m = c4d.Matrix()
        # scale Z to the proper ratio of slice resolution
        m.Scale(c4d.Vector(1,1,4))
        nr.SetMg(m)
        for i in range(len(linesList)):
            s = self.helper.spline(objprefix+'_spline'+str(i), linesList[i], close=0, type=c4d.SPLINETYPE_BSPLINE, scene=scn, parent=nr)
            print 'created spline ' + objprefix+'_spline'+str(i)
            # m = c4d.Matrix()
            # # scale Z to the proper ratio of slice resolution
            # m.Scale(c4d.Vector(1,1,4))
            # s[0].SetMg(m)


    def loadVTK(self, filename, objprefix, scale=0.1):
        if not filename : return
        vtkname,ext=os.path.splitext(os.path.basename(filename))
        print filename

        f = open(filename)
        lines = f.readlines()
        f.close()
        # print "closed file"

        vertexList = []
        edgeList = []

        lineNr = 0
        pattern = re.compile('([\d]+) ([\d]+) ([\d]+)')
        # print "ok 0"
        # read up to the POINTS string
        while "POINTS" not in lines[lineNr]:
            lineNr += 1
        # consume the POINTS line
        lineNr += 1
        # print "ok 1.0"
        # read POINTS up to the LINES string, 3 coordinates at a time
        while "LINES" not in lines[lineNr]:
            line_as_floats = [float(x) for x in lines[lineNr].split()]
            for i in range(len(line_as_floats)/3):
                vertexList.append((line_as_floats[i*3+0], line_as_floats[i*3+1], line_as_floats[i*3+2]))
            lineNr += 1
        # now read LINES to the end:
        # print "ok 1.1"
        while lineNr < len(lines)-1:
            lineNr += 1
            line_as_ints = [int(x) for x in lines[lineNr].split()]
            # print len(line_as_ints)
            # nrOfPoints = line_as_ints[0]
            for i in range(1, len(line_as_ints)-1):
                edgeList.append((line_as_ints[i], line_as_ints[i+1]))

        # print "ok 1.2"
        v = vertexList
        f = edgeList
        arr = c4d.BaseObject(c4d.Oatomarray)
        arr.SetName(objprefix+'_lineds')
        arr[1000] = scale #radius cylinder
        arr[1001] = scale #radius sphere
        arr[1002] = 16 #subdivision
        scn = self.helper.getCurrentScene()
        self.helper.addObjectToScene(scn,arr,parent=None)
        # print "ok 1.3"
        lines = self.helper.createsNmesh(objprefix+'_line',
                                         v,
                                         None,f)
        m = c4d.Matrix()
        # scale Z to the proper ratio of slice resolution
        m.Scale(c4d.Vector(1,1,4))
        lines[0].SetMg(m)

        # print "ok 2"
        self.helper.addObjectToScene(scn,lines[0],parent=arr)

    def loadMito(self, filename):
        self.loadVTK(filename, "Mito", 2.0)

    def loadTub(self, filename):
        self.loadVTK(filename, "Tub", 0.1)

    def loadGeneral(self, filename):
        self.loadVTK_bSplines(filename, "Test", 4.0)

    def browseVTK(self,*args):
        #first need to call the ui fileDialog
        self.fileDialog(label="choose a file", callback=self.loadGeneral, suffix="vtk")
        return True

    def browseMito(self,*args):
        #first need to call the ui fileDialog
        self.fileDialog(label="choose a file", callback=self.loadMito, suffix="vtk")
        return True

    def browseTub(self,*args):
        #first need to call the ui fileDialog
        self.fileDialog(label="choose a file", callback=self.loadTub, suffix="vtk")
        return True

    def initWidget(self,id=None):
        #this is where we define the buttons
        self.PUSHS = {}
        self.PUSHS["Browse"] = self._addElemt(name="Browse Raw VTK lines",width=40,height=10,
                         action=self.browseVTK,type="button")
        self.PUSHS["BrowseMito"] = self._addElemt(name="Browse Mito",width=40,height=10,
                         action=self.browseMito,type="button")
        self.PUSHS["BrowseTub"] = self._addElemt(name="Browse Microtubules",width=40,height=10,
                         action=self.browseTub,type="button")


    def setupLayout(self):
        #this where we define the Layout
        #this wil make three button on a row
        #self._layout is a class variable that you need to use
        #you may use your own, but remember to reassign to self._layou at the end.
        #for instance:
        #mylayout=[]
        #mylayout.append([widget])
        #self._layout=mylayout
        self._layout = []

        self._layout.append([self.PUSHS["Browse"]])

        #we creat a first "frame" for the first line of the dialog.
        #the frame can be collapse, and will present the creation button and the slider on the
        #same line
        elemFrame1=[]
        elemFrame1.append([self.PUSHS["BrowseMito"]])
        frame1 = self._addLayout(name="Mito",elems=elemFrame1)#,type="tab")
        #we add the frame to the layout
        self._layout.append(frame1)

        #we reapeat for the cube
        elemFrame2=[]
        elemFrame2.append([self.PUSHS["BrowseTub"]])
        frame2 = self._addLayout(name="Tubules",elems=elemFrame2)#,type="tab")
        self._layout.append(frame2)

#this is a script, we need some special code for the Tk and the Qt case.
#the most important part are the instanciation of our dialog class,
#and the two functio setup and display
#setup initialise the widget and the layout
#display wil actually show the dialog.
if uiadaptor.host == "tk":
    from DejaVu import Viewer
    vi = Viewer()
    #require a master
    #import Tkinter #Tkinter if python2.x tkinter for python3.x
    #root = Tkinter.Tk()
    mygui = VtkSegmentationReader(title="VTK Segmentation loader",master=vi)
    mygui.setup(vi=vi)
    #mygui.display()
elif uiadaptor.host == "qt":
    from PyQt4 import QtGui
    app = QtGui.QApplication(sys.argv)
    mygui = VtkSegmentationReader(title="VTK Segmentation loader")
    mygui.setup()
    #ex.show()
else :
    mygui = VtkSegmentationReader(title="VTK Segmentation loader")
    mygui.setup()
    #call it
mygui.display()
if uiadaptor.host == "qt": app.exec_()#work without it ?
